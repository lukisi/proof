/*
 *  This file is part of Netsukuku.
 *  Copyright (C) 2016 Luca Dionisi aka lukisi <luca.dionisi@gmail.com>
 *
 *  Netsukuku is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Netsukuku is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Netsukuku.  If not, see <http://www.gnu.org/licenses/>.
 */

using Gee;
using Netsukuku;
using Netsukuku.Neighborhood;
using Netsukuku.Identities;
using Netsukuku.Qspn;
using TaskletSystem;

namespace ProofOfConcept
{
    internal string json_string_object(Object obj)
    {
        Json.Node n = Json.gobject_serialize(obj);
        Json.Generator g = new Json.Generator();
        g.root = n;
        string ret = g.to_data(null);
        return ret;
    }

    const uint16 ntkd_port = 60269;
    const int max_paths = 5;
    const double max_common_hops_ratio = 0.6;
    const int arc_timeout = 10000;

    string naddr;
    string gsizes;
    [CCode (array_length = false, array_null_terminated = true)]
    string[] interfaces;
    bool accept_anonymous_requests;
    bool no_anonymize;
    int subnetlevel;

    ITasklet tasklet;
    Commander cm;
    TableNames tn;
    ArrayList<int> _gsizes;
    ArrayList<int> _g_exp;
    int levels;
    NeighborhoodManager? neighborhood_mgr;
    IdentityManager? identity_mgr;
    ArrayList<string> identity_mgr_arcs; // to memorize the `real_arcs` that have been added to IdentityManager
    ArrayList<string> real_nics;
    ArrayList<HandledNic> handlednics;
    int local_identity_nextindex;
    HashMap<int, IdentityData> local_identities;
    HashMap<string, INeighborhoodArc> neighborhood_arcs;
    HashMap<string, Arc> real_arcs;

    IdentityData find_or_create_local_identity(NodeID node_id)
    {
        foreach (int k in local_identities.keys)
        {
            NodeID local_nodeid = local_identities[k].nodeid;
            if (local_nodeid.equals(node_id))
            {
                return local_identities[k];
            }
        }
        int local_identity_index = local_identity_nextindex++;
        IdentityData ret = new IdentityData(node_id);
        local_identities[local_identity_index] = ret;
        ret.local_identity_index = local_identity_index;
        return ret;
    }

    void remove_local_identity(NodeID node_id)
    {
        foreach (int k in local_identities.keys)
        {
            NodeID local_nodeid = local_identities[k].nodeid;
            if (local_nodeid.equals(node_id))
            {
                local_identities.unset(k);
                return;
            }
        }
    }

    IdentityArc find_identity_arc(IdentityData identity_data, IIdmgmtArc arc, NodeID peer_nodeid)
    {
        foreach (IdentityArc ia in identity_data.identity_arcs.values)
        {
            if (ia.arc == arc)
             if (ia.id_arc.get_peer_nodeid().equals(peer_nodeid))
                return ia;
        }
        error("IdentityArc not found in identity_data.identity_arcs.values");
    }

    AddressManagerForNode node_skeleton;
    ServerDelegate dlg;
    ServerErrorHandler err;
    ArrayList<ITaskletHandle> t_udp_list;

    int main(string[] _args)
    {
        subnetlevel = 0; // default
        accept_anonymous_requests = false; // default
        no_anonymize = false; // default
        OptionContext oc = new OptionContext("init <topology> <address> | command ...");
        OptionEntry[] entries = new OptionEntry[5];
        int index = 0;
        entries[index++] = {"subnetlevel", 's', 0, OptionArg.INT, ref subnetlevel, "Level of g-node for autonomous subnet", null};
        entries[index++] = {"interfaces", 'i', 0, OptionArg.STRING_ARRAY, ref interfaces, "Interface (e.g. -i eth1). You can use it multiple times.", null};
        entries[index++] = {"serve-anonymous", 'k', 0, OptionArg.NONE, ref accept_anonymous_requests, "Accept anonymous requests", null};
        entries[index++] = {"no-anonymize", 'j', 0, OptionArg.NONE, ref no_anonymize, "Disable anonymizer", null};
        entries[index++] = { null };
        oc.add_main_entries(entries, null);
        try {
            oc.parse(ref _args);
        }
        catch (OptionError e) {
            print(@"Error parsing options: $(e.message)\n");
            return 1;
        }

        ArrayList<string> args = new ArrayList<string>.wrap(_args);
        if (args.size < 2) error("At least a command.");
        if (args[1] != "init")
        {
            if (args[1] == "help")
            {
                print(help_commands);
                return 0;
            }
            // A command to the instance running.
            // Initialize tasklet system
            PthTaskletImplementer.init();
            tasklet = PthTaskletImplementer.get_tasklet_system();
            // Open pipe for response in nonblock readonly.
            client_open_pipe_response();
            // generate a command id
            int id = Random.int_range(0, int.MAX);
            string command_id = @"$(id)";
            // concatenate command_id and command
            string cl = @"$(command_id)";
            for (int i = 1; i < args.size; i++) cl = @"$(cl) $(args[i])";
            try {
                // send command
                write_command(cl);
                // get response. first line is command_id, retval and number of lines.
                string resp0 = read_response();
                string prefix = @"$(command_id) ";
                assert(resp0.has_prefix(prefix));
                resp0 = resp0.substring(prefix.length);
                string[] resp0_pieces = resp0.split(" ");
                int retval = int.parse(resp0_pieces[0]);
                int numlines = int.parse(resp0_pieces[1]);
                for (int i = 0; i < numlines; i++)
                {
                    string resp = read_response();
                    print(@"$(resp)\n");
                }
                remove_pipe_response();
                PthTaskletImplementer.kill();
                return retval;
            } catch (Error e) {
                remove_pipe_response();
                error(@"Error during pass of command or response: $(e.message)");
            }
        }
        // `init` command.
        // Open pipe for commands in nonblock readonly.
        server_open_pipe_commands();
        args.remove_at(1);  // remove keyword `init` and go on as usual.

        if (args.size < 3) error("You have to set your topology and address as arguments after the keyword 'init'.");
        gsizes = args[1];
        naddr = args[2];
        ArrayList<int> _naddr = new ArrayList<int>();
        _gsizes = new ArrayList<int>();
        _g_exp = new ArrayList<int>();
        ArrayList<string> _devs = new ArrayList<string>();
        foreach (string s_piece in naddr.split(".")) _naddr.insert(0, int.parse(s_piece));
        foreach (string s_piece in gsizes.split("."))
        {
            int gsize = int.parse(s_piece);
            if (gsize < 2) error(@"Bad gsize $(gsize).");
            int gexp = 0;
            for (int k = 1; k < 17; k++)
            {
                if (gsize == (1 << k)) gexp = k;
            }
            if (gexp == 0) error(@"Bad gsize $(gsize): must be power of 2 up to 2^16.");
            _g_exp.insert(0, gexp);
            _gsizes.insert(0, gsize);
        }
        foreach (string dev in interfaces) _devs.add(dev);
        if (_naddr.size != _gsizes.size) error("You have to use same number of levels");
        levels = _gsizes.size;

        // Initialize tasklet system
        PthTaskletImplementer.init();
        tasklet = PthTaskletImplementer.get_tasklet_system();

        // Initialize known serializable classes
        // typeof(MyNodeID).class_peek();

        // TODO startup

        // Pass tasklet system to the RPC library (ntkdrpc)
        init_tasklet_system(tasklet);

        dlg = new ServerDelegate();
        err = new ServerErrorHandler();

        // Handle for TCP
        ITaskletHandle t_tcp;
        // Handles for UDP
        t_udp_list = new ArrayList<ITaskletHandle>();
        // Commander
        cm = Commander.get_singleton();
        cm.start_console_log();
        // TableNames
        tn = TableNames.get_singleton(cm);

        // start listen TCP
        t_tcp = tcp_listen(dlg, err, ntkd_port);

        string ntklocalhost = ip_internal_node(_naddr, 0);
        int bid = cm.begin_block();
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"sysctl", @"net.ipv4.ip_forward=1"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"sysctl", @"net.ipv4.conf.all.rp_filter=0"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"ip", @"address", @"add", @"$(ntklocalhost)", @"dev", @"lo"}));
        cm.end_block(bid);

        real_nics = new ArrayList<string>();
        handlednics = new ArrayList<HandledNic>();
        local_identity_nextindex = 0;
        local_identities = new HashMap<int, IdentityData>();

        neighborhood_arcs = new HashMap<string, INeighborhoodArc>();
        real_arcs = new HashMap<string, Arc>();
        pending_prepared_enter_net_operations = new HashMap<string,PreparedEnterNet>();
        pending_prepared_migrate_operations = new HashMap<string,PreparedMigrate>();

        // Init module Neighborhood
        NeighborhoodManager.init(tasklet);
        identity_mgr = null;
        node_skeleton = new AddressManagerForNode();
        neighborhood_mgr = new NeighborhoodManager(
            get_identity_skeleton,
            get_identity_skeleton_set,
            node_skeleton,
            1000 /*very high max_arcs*/,
            new NeighborhoodStubFactory(),
            new NeighborhoodIPRouteManager());
        node_skeleton.neighborhood_mgr = neighborhood_mgr;
        // connect signals
        neighborhood_mgr.nic_address_set.connect(neighborhood_nic_address_set);
        neighborhood_mgr.arc_added.connect(neighborhood_arc_added);
        neighborhood_mgr.arc_changed.connect(neighborhood_arc_changed);
        neighborhood_mgr.arc_removing.connect(neighborhood_arc_removing);
        neighborhood_mgr.arc_removed.connect(neighborhood_arc_removed);
        neighborhood_mgr.nic_address_unset.connect(neighborhood_nic_address_unset);
        foreach (string dev in _devs) manage_real_nic(dev);
        // Here (for each dev) the linklocal address has been added, and the signal handler for
        //  nic_address_set has been processed, so we have in `handlednics` the informations
        //  for the module Identities.
        Gee.List<string> if_list_dev = new ArrayList<string>();
        Gee.List<string> if_list_mac = new ArrayList<string>();
        Gee.List<string> if_list_linklocal = new ArrayList<string>();
        foreach (HandledNic n in handlednics)
        {
            if_list_dev.add(n.dev);
            if_list_mac.add(n.mac);
            if_list_linklocal.add(n.linklocal);
        }
        identity_mgr = new IdentityManager(
            tasklet,
            if_list_dev, if_list_mac, if_list_linklocal,
            new IdmgmtNetnsManager(),
            new IdmgmtStubFactory());
        identity_mgr_arcs = new ArrayList<string>();
        node_skeleton.identity_mgr = identity_mgr;
        identity_mgr.identity_arc_added.connect(identities_identity_arc_added);
        identity_mgr.identity_arc_changed.connect(identities_identity_arc_changed);
        identity_mgr.identity_arc_removing.connect(identities_identity_arc_removing);
        identity_mgr.identity_arc_removed.connect(identities_identity_arc_removed);
        identity_mgr.arc_removed.connect(identities_arc_removed);

        // First identity
        cm.single_command(new ArrayList<string>.wrap({
            @"ip", @"rule", @"add", @"table", @"ntk"}));

        NodeID nodeid = identity_mgr.get_main_id();
        IdentityData first_identity_data = find_or_create_local_identity(nodeid);
        Naddr my_naddr = new Naddr(_naddr.to_array(), _gsizes.to_array());
        ArrayList<int> _elderships = new ArrayList<int>();
        for (int i = 0; i < _gsizes.size; i++) _elderships.add(0);
        Fingerprint my_fp = new Fingerprint(_elderships.to_array());
        first_identity_data.my_naddr = my_naddr;
        first_identity_data.my_fp = my_fp;

        compute_local_ip_set(first_identity_data.local_ip_set, my_naddr);
        foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.global)", @"dev", @"$(dev)"}));
        if (accept_anonymous_requests)
        {
            foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.anonymous)", @"dev", @"$(dev)"}));
        }
        for (int i = levels-1; i >= 1; i--)
        {
            foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.intern[i])", @"dev", @"$(dev)"}));
        }

        bid = cm.begin_block();
        compute_destination_ip_set(first_identity_data.destination_ip_set, my_naddr);
        for (int i = levels-1; i >= subnetlevel; i--)
         for (int j = 0; j < _gsizes[i]; j++)
        {
            if (first_identity_data.destination_ip_set[i][j].global != "")
            {
                string ipaddr = first_identity_data.destination_ip_set[i][j].global;
                cm.single_command_in_block(bid, new ArrayList<string>.wrap({
                    @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"ntk"}));
                ipaddr = first_identity_data.destination_ip_set[i][j].anonymous;
                cm.single_command_in_block(bid, new ArrayList<string>.wrap({
                    @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"ntk"}));
            }
            for (int k = levels-1; k >= i+1; k--)
            {
                if (first_identity_data.destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = first_identity_data.destination_ip_set[i][j].intern[k];
                    cm.single_command_in_block(bid, new ArrayList<string>.wrap({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"ntk"}));
                }
            }
        }
        cm.end_block(bid);

        if (! no_anonymize)
        {
            string anonymousrange = ip_anonymizing_gnode(_naddr, levels);
            cm.single_command(new ArrayList<string>.wrap({
                @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$(anonymousrange)",
                @"-j", @"SNAT", @"--to", @"$(first_identity_data.local_ip_set.global)"}));
        }

        if (subnetlevel > 0)
        {
            string range1 = ip_internal_gnode(_naddr, subnetlevel, subnetlevel);
            for (int i = subnetlevel; i < levels; i++)
            {
                if (i < levels-1)
                {
                    string range2 = ip_internal_gnode(_naddr, subnetlevel, i+1);
                    string range3 = ip_internal_gnode(_naddr, i+1, i+1);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$(range2)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$(range3)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                }
                else
                {
                    string range2 = ip_global_gnode(_naddr, subnetlevel);
                    string range3 = ip_global_gnode(_naddr, levels);
                    string range4 = ip_anonymizing_gnode(_naddr, subnetlevel);
                    string range5 = ip_anonymizing_gnode(_naddr, levels);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$(range2)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$(range3)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                    if (accept_anonymous_requests) cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$(range4)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$(range5)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                }
            }
        }

        // First qspn manager
        print(@"$(get_time_now()): static Qspn.init.\n");
        QspnManager.init(tasklet, max_paths, max_common_hops_ratio, arc_timeout, new ThresholdCalculator(),
            (/*CallerInfo*/ rpc_caller, /*QspnManager*/ t) => {
                if (rpc_caller is TcpclientCallerInfo)
                {
                    TcpclientCallerInfo caller = (TcpclientCallerInfo)rpc_caller;
                    print(@"   Caller is TcpclientCallerInfo\n");
                    print(@"   my_address = $(caller.my_address)\n");
                    print(@"   peer_address = $(caller.peer_address)\n");
                    NodeID? sourceid = neighborhood_mgr.get_identity(caller.sourceid);
                    if (sourceid == null) print(@"   sourceid = null\n");
                    else print(@"   sourceid = $(sourceid.id)\n");
                }
                else if (rpc_caller is BroadcastCallerInfo)
                {
                    BroadcastCallerInfo caller = (BroadcastCallerInfo)rpc_caller;
                    print(@"   Caller is BroadcastCallerInfo\n");
                    print(@"   dev = $(caller.dev)\n");
                    print(@"   peer_address = $(caller.peer_address)\n");
                    NodeID? sourceid = neighborhood_mgr.get_identity(caller.sourceid);
                    if (sourceid == null) print(@"   sourceid = null\n");
                    else print(@"   sourceid = $(sourceid.id)\n");
                }
                else if (rpc_caller is UnicastCallerInfo)
                {
                    UnicastCallerInfo caller = (UnicastCallerInfo)rpc_caller;
                    print(@"   Caller is UnicastCallerInfo\n");
                    print(@"   dev = $(caller.dev)\n");
                    print(@"   peer_address = $(caller.peer_address)\n");
                    NodeID? sourceid = neighborhood_mgr.get_identity(caller.sourceid);
                    if (sourceid == null) print(@"   sourceid = null\n");
                    else print(@"   sourceid = $(sourceid.id)\n");
                }
                else
                {
                    assert_not_reached();
                }
            }
        );
        print(@"$(get_time_now()): Identity #$(first_identity_data.local_identity_index): construct Qspn.create_net.\n");
        {
            string _naddr_s = naddr_repr(my_naddr);
            string _elderships_s = fp_elderships_repr(my_fp);
            string _fp0_id_s = @"$(my_fp.id)";
            print(@"   my_naddr=$(_naddr_s) elderships=$(_elderships_s) fp0=$(_fp0_id_s) nodeid=$(first_identity_data.nodeid.id).\n");
        }
        QspnManager qspn_mgr = new QspnManager.create_net(
            my_naddr,
            my_fp,
            new QspnStubFactory(first_identity_data));
        // soon after creation, connect to signals.
        qspn_mgr.arc_removed.connect(first_identity_data.arc_removed);
        qspn_mgr.changed_fp.connect(first_identity_data.changed_fp);
        qspn_mgr.changed_nodes_inside.connect(first_identity_data.changed_nodes_inside);
        qspn_mgr.destination_added.connect(first_identity_data.destination_added);
        qspn_mgr.destination_removed.connect(first_identity_data.destination_removed);
        qspn_mgr.gnode_splitted.connect(first_identity_data.gnode_splitted);
        qspn_mgr.path_added.connect(first_identity_data.path_added);
        qspn_mgr.path_changed.connect(first_identity_data.path_changed);
        qspn_mgr.path_removed.connect(first_identity_data.path_removed);
        qspn_mgr.presence_notified.connect(first_identity_data.presence_notified);
        qspn_mgr.qspn_bootstrap_complete.connect(first_identity_data.qspn_bootstrap_complete);
        qspn_mgr.remove_identity.connect(first_identity_data.remove_identity);

        identity_mgr.set_identity_module(nodeid, "qspn", qspn_mgr);
        first_identity_data.addr_man = new AddressManagerForIdentity(qspn_mgr);
        qspn_mgr = null;

        foreach (string s in print_local_identity(0)) print(s + "\n");

        // end startup

        // start a tasklet to get commands from pipe_commands.
        ReadCommandsTasklet ts = new ReadCommandsTasklet();
        ITaskletHandle h_read_commands = tasklet.spawn(ts);

        // start a tasklet to periodically update all routes.
        UpdateAllRoutesTasklet ts_up = new UpdateAllRoutesTasklet();
        ITaskletHandle h_update_all_routes = tasklet.spawn(ts_up);

        // register handlers for SIGINT and SIGTERM to exit
        Posix.@signal(Posix.SIGINT, safe_exit);
        Posix.@signal(Posix.SIGTERM, safe_exit);
        // Main loop
        while (true)
        {
            tasklet.ms_wait(100);
            if (do_me_exit) break;
        }
        h_read_commands.kill();
        remove_pipe_commands();
        h_update_all_routes.kill();

        // TODO cleanup

        // First, we call stop_monitor_all of NeighborhoodManager.
        neighborhood_mgr.stop_monitor_all();

        // Remove connectivity identities and their network namespaces and linklocal addresses.
        ArrayList<int> local_identities_keys = new ArrayList<int>();
        local_identities_keys.add_all(local_identities.keys);
        foreach (int i in local_identities_keys)
        {
            IdentityData identity_data = local_identities[i];
            if (! identity_data.main_id)
            {
                // TODO remove namespace
                // TODO when needed, remove ntk_from_xxx from rt_tables
                identity_mgr.remove_identity(identity_data.nodeid);
                local_identities.unset(i);
            }
        }

        // For main identity...
        assert(local_identities.size == 1);
        int kk = -1;
        foreach (int k in local_identities.keys) kk = k;
        IdentityData identity_data = local_identities[kk];
        assert(identity_data.main_id);
        // ... disconnect signal handlers of qspn_mgr.
        qspn_mgr = (QspnManager)identity_mgr.get_identity_module(identity_data.nodeid, "qspn");
        qspn_mgr.arc_removed.disconnect(identity_data.arc_removed);
        qspn_mgr.changed_fp.disconnect(identity_data.changed_fp);
        qspn_mgr.changed_nodes_inside.disconnect(identity_data.changed_nodes_inside);
        qspn_mgr.destination_added.disconnect(identity_data.destination_added);
        qspn_mgr.destination_removed.disconnect(identity_data.destination_removed);
        qspn_mgr.gnode_splitted.disconnect(identity_data.gnode_splitted);
        qspn_mgr.path_added.disconnect(identity_data.path_added);
        qspn_mgr.path_changed.disconnect(identity_data.path_changed);
        qspn_mgr.path_removed.disconnect(identity_data.path_removed);
        qspn_mgr.presence_notified.disconnect(identity_data.presence_notified);
        qspn_mgr.qspn_bootstrap_complete.disconnect(identity_data.qspn_bootstrap_complete);
        qspn_mgr.remove_identity.disconnect(identity_data.remove_identity);
        print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): disabling handlers for Qspn signals.\n");
        identity_data.qspn_handlers_disabled = true;
        identity_mgr.unset_identity_module(identity_data.nodeid, "qspn");
        print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): call stop_operations.\n");
        qspn_mgr.stop_operations();
        qspn_mgr = null;

        // Cleanup addresses and routes that were added previously in order to
        //  obey to the qspn_mgr which is now in default network namespace.
        // TODO foreach table ntk_from_xxx: remove rule, flush, remove from rt_tables
        // remove rule ntk
        cm.single_command(new ArrayList<string>.wrap({
            @"ip", @"rule", @"del", @"table", @"ntk"}));
        // flush table ntk
        cm.single_command(new ArrayList<string>.wrap({
            @"ip", @"route", @"flush", @"table", @"ntk"}));

        // remove SNAT rule
        if (! no_anonymize && identity_data.local_ip_set.global != "")
        {
            string anonymousrange = ip_anonymizing_gnode(identity_data.my_naddr.pos, levels);
            cm.single_command(new ArrayList<string>.wrap({
                @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$(anonymousrange)",
                @"-j", @"SNAT", @"--to", @"$(identity_data.local_ip_set.global)"}));
        }

        // remove local addresses (global, anon, intern, localhost)
        if (identity_data.local_ip_set.global != "")
            foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"del", @"$(identity_data.local_ip_set.global)/32", @"dev", @"$(dev)"}));
        if (identity_data.local_ip_set.anonymous != "" && accept_anonymous_requests)
            foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"del", @"$(identity_data.local_ip_set.anonymous)/32", @"dev", @"$(dev)"}));
        for (int i = levels-1; i >= 1; i--)
        {
            if (identity_data.local_ip_set.intern[i] != "")
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(identity_data.local_ip_set.intern[i])/32", @"dev", @"$(dev)"}));
        }
        cm.single_command(new ArrayList<string>.wrap({
            @"ip", @"address", @"del", @"$(ntklocalhost)/32", @"dev", @"lo"}));

        // remove NETMAP rules
        if (subnetlevel > 0)
        {
            string range1 = ip_internal_gnode(_naddr, subnetlevel, subnetlevel);
            for (int i = subnetlevel; i < levels; i++)
            {
                if (identity_data.my_naddr.pos[i] >= _gsizes[i]) break;
                if (i < levels-1)
                {
                    string range2 = ip_internal_gnode(_naddr, subnetlevel, i+1);
                    string range3 = ip_internal_gnode(_naddr, i+1, i+1);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$(range2)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$(range3)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                }
                else
                {
                    string range2 = ip_global_gnode(_naddr, subnetlevel);
                    string range3 = ip_global_gnode(_naddr, levels);
                    string range4 = ip_anonymizing_gnode(_naddr, subnetlevel);
                    string range5 = ip_anonymizing_gnode(_naddr, levels);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$(range2)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$(range3)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                    if (accept_anonymous_requests) cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$(range4)",
                        @"-j", @"NETMAP", @"--to", @"$(range1)"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$(range5)", @"-s", @"$(range1)",
                        @"-j", @"NETMAP", @"--to", @"$(range2)"}));
                }
            }
        }

        // Then we destroy the object NeighborhoodManager.
        // Beware that node_skeleton.neighborhood_mgr is a weak reference.
        neighborhood_mgr = null;

        foreach (ITaskletHandle t_udp in t_udp_list) t_udp.kill();
        t_tcp.kill();

        tasklet.ms_wait(100);

        PthTaskletImplementer.kill();
        print("\nExiting.\n");
        return 0;
    }

    bool do_me_exit = false;
    void safe_exit(int sig)
    {
        // We got here because of a signal. Quick processing.
        do_me_exit = true;
    }

    void manage_real_nic(string dev)
    {
        real_nics.add(dev);

        int bid = cm.begin_block();
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"sysctl", @"net.ipv4.conf.$(dev).rp_filter=0"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"sysctl", @"net.ipv4.conf.$(dev).arp_ignore=1"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"sysctl", @"net.ipv4.conf.$(dev).arp_announce=2"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap({
            @"ip", @"link", @"set", @"dev", @"$(dev)", @"up"}));
        cm.end_block(bid);

        // Start listen UDP on dev
        t_udp_list.add(udp_listen(dlg, err, ntkd_port, dev));
        // Run monitor
        neighborhood_mgr.start_monitor(new NeighborhoodNetworkInterface(dev));
    }

    class HandledNic : Object
    {
        public string dev;
        public string mac;
        public string linklocal;
    }

    class Arc : Object
    {
        public int cost;
        public INeighborhoodArc neighborhood_arc;
        public IdmgmtArc idmgmt_arc;
    }

    class IdentityArc : Object
    {
        public IIdmgmtArc arc;
        public NodeID id;
        public IIdmgmtIdentityArc id_arc;
        public weak IdentityData identity_data;
        public string peer_mac;
        public string peer_linklocal;
        public QspnArc? qspn_arc;
        public string? tablename;
        public int? tid;
        public bool? rule_added;
        public string? prev_peer_mac;
        public string? prev_peer_linklocal;
        public string? prev_tablename;
        public int? prev_tid;
        public bool? prev_rule_added;
        public int identity_arc_index;
        public IdentityArc(IdentityData identity_data, IIdmgmtArc arc, IIdmgmtIdentityArc id_arc)
        {
            this.identity_data = identity_data;
            this.arc = arc;
            id = identity_data.nodeid;
            this.id_arc = id_arc;
            peer_mac = id_arc.get_peer_mac();
            peer_linklocal = id_arc.get_peer_linklocal();

            identity_arc_index = identity_data.identity_arc_nextindex++;
            identity_data.identity_arcs[identity_arc_index] = this;

            qspn_arc = null;
            tablename = null;
            tid = null;
            rule_added = null;
            prev_peer_mac = null;
            prev_peer_linklocal = null;
            prev_tablename = null;
            prev_tid = null;
            prev_rule_added = null;
        }
    }

    class LocalIPSet : Object
    {
        public string global;
        public string anonymous;
        public HashMap<int,string> intern;
    }

    LocalIPSet init_local_ip_set()
    {
        LocalIPSet local_ip_set = new LocalIPSet();
        local_ip_set.global = "";
        local_ip_set.anonymous = "";
        local_ip_set.intern = new HashMap<int,string>();
        for (int j = 1; j < levels; j++) local_ip_set.intern[j] = "";
        return local_ip_set;
    }

    LocalIPSet copy_local_ip_set(LocalIPSet orig)
    {
        LocalIPSet ret = new LocalIPSet();
        ret.global = orig.global;
        ret.anonymous = orig.anonymous;
        ret.intern = new HashMap<int,string>();
        for (int k = 1; k < levels; k++)
            ret.intern[k] = orig.intern[k];
        return ret;
    }

    void compute_local_ip_set(LocalIPSet local_ip_set, Naddr my_naddr)
    {
        if (my_naddr.is_real_from_to(0, levels-1))
        {
            local_ip_set.global = ip_global_node(my_naddr.pos);
            local_ip_set.anonymous = ip_anonymizing_node(my_naddr.pos);
        }
        else
        {
            local_ip_set.global = "";
            local_ip_set.anonymous = "";
        }
        for (int i = levels-1; i >= 1; i--)
        {
            if (my_naddr.is_real_from_to(0, i-1))
                local_ip_set.intern[i] = ip_internal_node(my_naddr.pos, i);
            else
                local_ip_set.intern[i] = "";
        }
    }

    class DestinationIPSet : Object
    {
        public string global;
        public string anonymous;
        public HashMap<int,string> intern;
    }

    HashMap<int,HashMap<int,DestinationIPSet>> init_destination_ip_set()
    {
        HashMap<int,HashMap<int,DestinationIPSet>> ret;
        ret = new HashMap<int,HashMap<int,DestinationIPSet>>();
        for (int i = subnetlevel; i < levels; i++)
        {
            ret[i] = new HashMap<int,DestinationIPSet>();
            for (int j = 0; j < _gsizes[i]; j++)
            {
                ret[i][j] = new DestinationIPSet();
                ret[i][j].global = "";
                ret[i][j].anonymous = "";
                ret[i][j].intern = new HashMap<int,string>();
                for (int k = i + 1; k < levels; k++) ret[i][j].intern[k] = "";
            }
        }
        return ret;
    }

    HashMap<int,HashMap<int,DestinationIPSet>> copy_destination_ip_set(HashMap<int,HashMap<int,DestinationIPSet>> orig)
    {
        HashMap<int,HashMap<int,DestinationIPSet>> ret;
        ret = new HashMap<int,HashMap<int,DestinationIPSet>>();
        for (int i = subnetlevel; i < levels; i++)
        {
            ret[i] = new HashMap<int,DestinationIPSet>();
            for (int j = 0; j < _gsizes[i]; j++)
            {
                ret[i][j] = new DestinationIPSet();
                ret[i][j].global = orig[i][j].global;
                ret[i][j].anonymous = orig[i][j].anonymous;
                ret[i][j].intern = new HashMap<int,string>();
                for (int k = i + 1; k < levels; k++)
                    ret[i][j].intern[k] = orig[i][j].intern[k];
            }
        }
        return ret;
    }

    void compute_destination_ip_set(HashMap<int,HashMap<int,DestinationIPSet>> destination_ip_set, Naddr my_naddr)
    {
        for (int i = subnetlevel; i < levels; i++)
         for (int j = 0; j < _gsizes[i]; j++)
        {
            ArrayList<int> naddr = new ArrayList<int>();
            naddr.add_all(my_naddr.pos);
            naddr[i] = j;
            if (my_naddr.is_real_from_to(i+1, levels-1) && my_naddr.pos[i] != j)
            {
                destination_ip_set[i][j].global = ip_global_gnode(naddr, i);
                destination_ip_set[i][j].anonymous = ip_anonymizing_gnode(naddr, i);
            }
            else
            {
                destination_ip_set[i][j].global = "";
                destination_ip_set[i][j].anonymous = "";
            }
            for (int k = i + 1; k < levels; k++)
            {
                if (my_naddr.is_real_from_to(i+1, k-1) && my_naddr.pos[i] != j)
                {
                    destination_ip_set[i][j].intern[k] = ip_internal_gnode(naddr, i, k);
                }
                else
                {
                    destination_ip_set[i][j].intern[k] = "";
                }
            }
        }
    }

    class IdentityData : Object
    {

        public IdentityData(NodeID nodeid)
        {
            this.nodeid = nodeid;
            identity_arc_nextindex = 0;
            identity_arcs = new HashMap<int, IdentityArc>();
            connectivity_from_level = 0;
            connectivity_to_level = 0;
            copy_of_identity = null;

            local_ip_set = init_local_ip_set();

            destination_ip_set = init_destination_ip_set();
        }

        public NodeID nodeid;
        public Naddr my_naddr;
        public Fingerprint my_fp;
        public int connectivity_from_level;
        public int connectivity_to_level;

        public IdentityData? copy_of_identity;
        public int local_identity_index;
        public AddressManagerForIdentity addr_man;

        public int identity_arc_nextindex;
        public HashMap<int, IdentityArc> identity_arcs;

        public LocalIPSet local_ip_set;
        public HashMap<int,HashMap<int,DestinationIPSet>> destination_ip_set;

        private string _network_namespace;
        public string network_namespace {
            get {
                _network_namespace = identity_mgr.get_namespace(nodeid);
                return _network_namespace;
            }
        }

        public bool main_id {
            get {
                return network_namespace == "";
            }
        }

        // handle signals from qspn_manager

        public bool qspn_handlers_disabled = false;

        public void arc_removed(IQspnArc arc, string message, bool bad_link)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.arc_removed.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            {
                QspnArc qspnarc = (QspnArc)arc;
                Arc real_arc = qspnarc.arc;
                print(@"   Real arc is through $(real_arc.neighborhood_arc.nic.dev) to $(real_arc.neighborhood_arc.neighbour_mac).\n");
                print(@"   Identity arc is from $(qspnarc.sourceid.id) to $(qspnarc.destid.id).\n");
            }
            per_identity_qspn_arc_removed(this, arc, message, bad_link);
        }

        public void changed_fp(int l)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.changed_fp.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            print(@"   At level $(l).\n");
            {
                QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
                try {
                    Fingerprint fp_l = (Fingerprint)qspn_mgr.get_fingerprint(l);
                    print(@"   Fingerprint $(fp_l.id), " +
                        @"elderships $(fp_elderships_repr(fp_l)).\n");
                } catch (QspnBootstrapInProgressError e) {
                    print(@"   No more info because QspnBootstrapInProgressError at that level.\n");
                }
            }
            per_identity_qspn_changed_fp(this, l);
        }

        public void changed_nodes_inside(int l)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.changed_nodes_inside.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            print(@"   At level $(l).\n");
            {
                QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
                try {
                    int nodes_inside_l = qspn_mgr.get_nodes_inside(l);
                    print(@"   Nodes inside #$(nodes_inside_l).\n");
                } catch (QspnBootstrapInProgressError e) {
                    print(@"   No more info because QspnBootstrapInProgressError at that level.\n");
                }
            }
            per_identity_qspn_changed_nodes_inside(this, l);
        }

        public void destination_added(HCoord h)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.destination_added.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            print(@"   Destination to ($(h.lvl), $(h.pos)).\n");
            per_identity_qspn_destination_added(this, h);
        }

        public void destination_removed(HCoord h)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.destination_removed.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            print(@"   Destination to ($(h.lvl), $(h.pos)).\n");
            per_identity_qspn_destination_removed(this, h);
        }

        public void gnode_splitted(IQspnArc a, HCoord d, IQspnFingerprint fp)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.gnode_splitted.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            per_identity_qspn_gnode_splitted(this, a, d, fp);
        }

        public void path_added(IQspnNodePath p)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.path_added.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            {
                QspnArc arc = (QspnArc)p.i_qspn_get_arc();
                Arc real_arc = arc.arc;
                print(@"   Real arc is through $(real_arc.neighborhood_arc.nic.dev) to $(real_arc.neighborhood_arc.neighbour_mac).\n");
                print(@"   Identity arc is from $(arc.sourceid.id) to $(arc.destid.id).\n");
                Cost arc_c = (Cost)arc.i_qspn_get_cost();
                print(@"   Arc cost is $(arc_c.usec_rtt) usec.\n");
                Cost c = (Cost)p.i_qspn_get_cost();
                print(@"   Path cost is $(c.usec_rtt) usec.\n");
                print(@"   Number of nodes inside is $(p.i_qspn_get_nodes_inside()).\n");
                string hops = ""; string sep = "";
                foreach (IQspnHop hop in p.i_qspn_get_hops())
                {
                    HCoord hop_h = hop.i_qspn_get_hcoord();
                    int hop_arcid = hop.i_qspn_get_arc_id();
                    hops += @"$(sep)arc $(hop_arcid) to ($(hop_h.lvl), $(hop_h.pos))";
                    sep = ", ";
                }
                print(@"   Path: [$(hops)].\n");
            }
            per_identity_qspn_path_added(this, p);
        }

        public void path_changed(IQspnNodePath p)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.path_changed.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            {
                QspnArc arc = (QspnArc)p.i_qspn_get_arc();
                Arc real_arc = arc.arc;
                print(@"   Real arc is through $(real_arc.neighborhood_arc.nic.dev) to $(real_arc.neighborhood_arc.neighbour_mac).\n");
                print(@"   Identity arc is from $(arc.sourceid.id) to $(arc.destid.id).\n");
                Cost arc_c = (Cost)arc.i_qspn_get_cost();
                print(@"   Arc cost is $(arc_c.usec_rtt) usec.\n");
                Cost c = (Cost)p.i_qspn_get_cost();
                print(@"   Path cost is $(c.usec_rtt) usec.\n");
                print(@"   Number of nodes inside is $(p.i_qspn_get_nodes_inside()).\n");
                string hops = ""; string sep = "";
                foreach (IQspnHop hop in p.i_qspn_get_hops())
                {
                    HCoord hop_h = hop.i_qspn_get_hcoord();
                    int hop_arcid = hop.i_qspn_get_arc_id();
                    hops += @"$(sep)arc $(hop_arcid) to ($(hop_h.lvl), $(hop_h.pos))";
                    sep = ", ";
                }
                print(@"   Path: [$(hops)].\n");
            }
            per_identity_qspn_path_changed(this, p);
        }

        public void path_removed(IQspnNodePath p)
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.path_removed.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            {
                QspnArc arc = (QspnArc)p.i_qspn_get_arc();
                Arc real_arc = arc.arc;
                print(@"   Real arc is through $(real_arc.neighborhood_arc.nic.dev) to $(real_arc.neighborhood_arc.neighbour_mac).\n");
                print(@"   Identity arc is from $(arc.sourceid.id) to $(arc.destid.id).\n");
                Cost arc_c = (Cost)arc.i_qspn_get_cost();
                print(@"   Arc cost is $(arc_c.usec_rtt) usec.\n");
                Cost c = (Cost)p.i_qspn_get_cost();
                print(@"   Path cost is $(c.usec_rtt) usec.\n");
                print(@"   Number of nodes inside is $(p.i_qspn_get_nodes_inside()).\n");
                string hops = ""; string sep = "";
                foreach (IQspnHop hop in p.i_qspn_get_hops())
                {
                    HCoord hop_h = hop.i_qspn_get_hcoord();
                    int hop_arcid = hop.i_qspn_get_arc_id();
                    hops += @"$(sep)arc $(hop_arcid) to ($(hop_h.lvl), $(hop_h.pos))";
                    sep = ", ";
                }
                print(@"   Path: [$(hops)].\n");
            }
            per_identity_qspn_path_removed(this, p);
        }

        public void presence_notified()
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.presence_notified.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            per_identity_qspn_presence_notified(this);
        }

        public void qspn_bootstrap_complete()
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.qspn_bootstrap_complete.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            {
                QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
                for (int i = 1; i <= levels; i++)
                {
                    try {
                        print(@"   Level $(i): calling get_fingerprint($(i)) and get_nodes_inside($(i)) ...\n");
                        Fingerprint fp_i = (Fingerprint)qspn_mgr.get_fingerprint(i);
                        int nodes_inside_i = qspn_mgr.get_nodes_inside(i);
                        print(@"   Level $(i): Fingerprint $(fp_i.id), " +
                            @"elderships $(fp_elderships_repr(fp_i)). " +
                            @"Nodes inside #$(nodes_inside_i).\n");
                    } catch (QspnBootstrapInProgressError e) {
                        assert_not_reached();
                    }
                }
            }
            per_identity_qspn_qspn_bootstrap_complete(this);
        }

        public void remove_identity()
        {
            print(@"$(get_time_now()): Identity #$(local_identity_index): signal Qspn.remove_identity.\n");
            if (qspn_handlers_disabled)
            {
                print("   Handlers have been disabled for this identity.\n");
                return;
            }
            per_identity_qspn_remove_identity(this);
        }
    }

    class NeighborhoodIPRouteManager : Object, INeighborhoodIPRouteManager
    {
        public void add_address(string my_addr, string my_dev)
        {
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"add", @"$(my_addr)", @"dev", @"$(my_dev)"}));
        }

        public void add_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"route", @"add", @"$(neighbor_addr)", @"dev", @"$(my_dev)", @"src", @"$(my_addr)"}));
        }

        public void remove_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"route", @"del", @"$(neighbor_addr)", @"dev", @"$(my_dev)", @"src", @"$(my_addr)"}));
        }

        public void remove_address(string my_addr, string my_dev)
        {
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"del", @"$(my_addr)/32", @"dev", @"$(my_dev)"}));
        }
    }

    class NeighborhoodStubFactory : Object, INeighborhoodStubFactory
    {
        public IAddressManagerStub
        get_broadcast(
            Gee.List<string> devs,
            Gee.List<string> src_ips,
            ISourceID source_id,
            IBroadcastID broadcast_id,
            IAckCommunicator? ack_com = null)
        {
            assert(! devs.is_empty);
            assert(devs.size == src_ips.size);
            var bc = get_addr_broadcast(devs, src_ips, ntkd_port, source_id, broadcast_id, ack_com);
            return bc;
        }

        public IAddressManagerStub
        get_unicast(
            string dev,
            string src_ip,
            ISourceID source_id,
            IUnicastID unicast_id,
            bool wait_reply = true)
        {
            var uc = get_addr_unicast(dev, ntkd_port, src_ip, source_id, unicast_id, wait_reply);
            return uc;
        }

        public IAddressManagerStub
        get_tcp(
            string dest,
            ISourceID source_id,
            IUnicastID unicast_id,
            bool wait_reply = true)
        {
            var tc = get_addr_tcp_client(dest, ntkd_port, source_id, unicast_id);
            assert(tc is ITcpClientRootStub);
            ((ITcpClientRootStub)tc).wait_reply = wait_reply;
            return tc;
        }
    }

    class NeighborhoodNetworkInterface : Object, INeighborhoodNetworkInterface
    {
        public NeighborhoodNetworkInterface(string dev)
        {
            _dev = dev;
            _mac = macgetter.get_mac(dev).up();
        }
        private string _dev;
        private string _mac;

        public string dev {
            get {
                return _dev;
            }
        }

        public string mac {
            get {
                return _mac;
            }
        }

        public long measure_rtt(string peer_addr, string peer_mac, string my_dev, string my_addr) throws NeighborhoodGetRttError
        {
            TaskletCommandResult com_ret;
            try {
                //print(@"ping -n -q -c 1 $(peer_addr)\n");
                com_ret = tasklet.exec_command(@"ping -n -q -c 1 $(peer_addr)");
            } catch (Error e) {
                throw new NeighborhoodGetRttError.GENERIC(@"Unable to spawn a command: $(e.message)");
            }
            if (com_ret.exit_status != 0)
                throw new NeighborhoodGetRttError.GENERIC(@"ping: error $(com_ret.stdout)");
            foreach (string line in com_ret.stdout.split("\n"))
            {
                /*  """rtt min/avg/max/mdev = 2.854/2.854/2.854/0.000 ms"""  */
                if (line.has_prefix("rtt ") && line.has_suffix(" ms"))
                {
                    string s2 = line.substring(line.index_of(" = ") + 3);
                    string s3 = s2.substring(0, s2.index_of("/"));
                    double x;
                    bool res = double.try_parse (s3, out x);
                    if (res)
                    {
                        long ret = (long)(x * 1000);
                        //print(@" returned $(ret) microseconds.\n");
                        return ret;
                    }
                }
            }
            throw new NeighborhoodGetRttError.GENERIC(@"could not parse $(com_ret.stdout)");
        }
    }

    class NeighborhoodMissingArcHandler : Object, INeighborhoodMissingArcHandler
    {
        public NeighborhoodMissingArcHandler.from_qspn(IQspnMissingArcHandler qspn_missing, IdentityData identity_data)
        {
            this.qspn_missing = qspn_missing;
            this.identity_data = identity_data;
        }
        private IQspnMissingArcHandler? qspn_missing;
        private weak IdentityData identity_data;

        public void missing(INeighborhoodArc arc)
        {
            if (qspn_missing != null)
            {
                // from a INeighborhoodArc get a list of QspnArc
                foreach (IdentityArc ia in identity_data.identity_arcs.values) if (ia.qspn_arc != null)
                    if (ia.qspn_arc.arc.neighborhood_arc == arc)
                        qspn_missing.i_qspn_missing(ia.qspn_arc);
            }
        }
    }

    IAddressManagerSkeleton?
    get_identity_skeleton(
        NodeID source_id,
        NodeID unicast_id,
        string peer_address)
    {
        foreach (int local_identity_index in local_identities.keys)
        {
            IdentityData local_identity_data = local_identities[local_identity_index];
            NodeID local_nodeid = local_identity_data.nodeid;
            if (local_nodeid.equals(unicast_id))
            {
                foreach (IdentityArc ia in local_identity_data.identity_arcs.values)
                {
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address)
                    {
                        if (ia.id_arc.get_peer_nodeid().equals(source_id))
                        {
                            return local_identity_data.addr_man;
                        }
                    }
                }
            }
        }
        return null;
    }

    Gee.List<IAddressManagerSkeleton>
    get_identity_skeleton_set(
        NodeID source_id,
        Gee.List<NodeID> broadcast_set,
        string peer_address,
        string dev)
    {
        ArrayList<IAddressManagerSkeleton> ret = new ArrayList<IAddressManagerSkeleton>();
        foreach (int local_identity_index in local_identities.keys)
        {
            IdentityData local_identity_data = local_identities[local_identity_index];
            NodeID local_nodeid = local_identity_data.nodeid;
            if (local_nodeid in broadcast_set)
            {
                foreach (IdentityArc ia in local_identity_data.identity_arcs.values)
                {
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address
                        && _arc.neighborhood_arc.nic.dev == dev)
                    {
                        if (ia.id_arc.get_peer_nodeid().equals(source_id))
                        {
                            ret.add(local_identity_data.addr_man);
                        }
                    }
                }
            }
        }
        return ret;
    }

    class IdmgmtNetnsManager : Object, IIdmgmtNetnsManager
    {
        private HashMap<string, string> pseudo_macs;
        public IdmgmtNetnsManager()
        {
            pseudo_macs = new HashMap<string, string>();
        }

        public void create_namespace(string ns)
        {
            assert(ns != "");
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"add", @"$(ns)"}));
            cm.single_command(new ArrayList<string>.wrap({
                @"sysctl", @"net.ipv4.ip_forward=1"}));
            cm.single_command(new ArrayList<string>.wrap({
                @"sysctl", @"net.ipv4.conf.all.rp_filter=0"}));
        }

        public void create_pseudodev(string dev, string ns, string pseudo_dev, out string pseudo_mac)
        {
            assert(ns != "");
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"link", @"add", @"dev", @"$(pseudo_dev)", @"link", @"$(dev)", @"type", @"macvlan"}));
            pseudo_mac = macgetter.get_mac(pseudo_dev).up();
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"link", @"set", @"dev", @"$(pseudo_dev)", @"netns", @"$(ns)"}));
            // disable rp_filter
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"sysctl", @"net.ipv4.conf.$(pseudo_dev).rp_filter=0"}));
            // arp policies
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"sysctl", @"net.ipv4.conf.$(pseudo_dev).arp_ignore=1"}));
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"sysctl", @"net.ipv4.conf.$(pseudo_dev).arp_announce=2"}));
            // up
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"ip", @"link", @"set", @"dev", @"$(pseudo_dev)", @"up"}));
            assert(! pseudo_macs.has_key(pseudo_dev));
            pseudo_macs[pseudo_dev] = pseudo_mac;
        }

        public void add_address(string ns, string pseudo_dev, string linklocal)
        {
            // ns may be empty-string.
            ArrayList<string> argv = new ArrayList<string>();
            if (ns != "") argv.add_all_array({@"ip", @"netns", @"exec", @"$(ns)"});
            argv.add_all_array({
                @"ip", @"address", @"add", @"$(linklocal)", @"dev", @"$(pseudo_dev)"});
            cm.single_command(argv);
        }

        public void add_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            // ns may be empty-string.
            ArrayList<string> argv = new ArrayList<string>();
            if (ns != "") argv.add_all_array({@"ip", @"netns", @"exec", @"$(ns)"});
            argv.add_all_array({
                @"ip", @"route", @"add", @"$(linklocal_dst)", @"dev", @"$(dev)", @"src", @"$(linklocal_src)"});
            cm.single_command(argv);
        }

        public void remove_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            // ns may be empty-string.
            ArrayList<string> argv = new ArrayList<string>();
            if (ns != "") argv.add_all_array({@"ip", @"netns", @"exec", @"$(ns)"});
            argv.add_all_array({
                @"ip", @"route", @"del", @"$(linklocal_dst)", @"dev", @"$(dev)", @"src", @"$(linklocal_src)"});
            cm.single_command(argv);
        }

        public void flush_table(string ns)
        {
            assert(ns != "");
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"ip", @"route", @"flush", @"table", @"main"}));
        }

        public void delete_pseudodev(string ns, string pseudo_dev)
        {
            assert(ns != "");
            if (pseudo_macs.has_key(pseudo_dev)) pseudo_macs.unset(pseudo_dev);
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"exec", @"$(ns)", @"ip", @"link", @"delete", @"$(pseudo_dev)", @"type", @"macvlan"}));
        }

        public void delete_namespace(string ns)
        {
            assert(ns != "");
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"netns", @"del", @"$(ns)"}));
        }
    }

    class IdmgmtStubFactory : Object, IIdmgmtStubFactory
    {
        /* This "holder" class is needed because the IdentityManagerRemote class provided by
         * the ZCD framework is owned (and tied to) by the AddressManagerXxxxRootStub.
         */
        private class IdentityManagerStubHolder : Object, IIdentityManagerStub
        {
            public IdentityManagerStubHolder(IAddressManagerStub addr)
            {
                this.addr = addr;
            }
            private IAddressManagerStub addr;

            public IIdentityID get_peer_main_id()
            throws StubError, DeserializeError
            {
                return addr.identity_manager.get_peer_main_id();
            }

            public IDuplicationData? match_duplication
            (int migration_id, IIdentityID peer_id, IIdentityID old_id,
            IIdentityID new_id, string old_id_new_mac, string old_id_new_linklocal)
            throws StubError, DeserializeError
            {
                return addr.identity_manager.match_duplication
                    (migration_id, peer_id, old_id,
                     new_id, old_id_new_mac, old_id_new_linklocal);
            }

            public void notify_identity_arc_removed(IIdentityID peer_id, IIdentityID my_id)
            throws StubError, DeserializeError
            {
                addr.identity_manager.notify_identity_arc_removed(peer_id, my_id);
            }
        }

        public IIdmgmtArc? get_arc(CallerInfo caller)
        {
            if (caller is TcpclientCallerInfo)
            {
                TcpclientCallerInfo c = (TcpclientCallerInfo)caller;
                ISourceID sourceid = c.sourceid;
                string my_address = c.my_address;
                foreach (HandledNic n in handlednics)
                {
                    string dev = n.dev;
                    if (n.linklocal == my_address)
                    {
                        INeighborhoodArc? neighborhood_arc = neighborhood_mgr.get_node_arc(sourceid, dev);
                        if (neighborhood_arc == null)
                        {
                            // some warning message?
                            return null;
                        }
                        foreach (string k in real_arcs.keys)
                        {
                            Arc arc = real_arcs[k];
                            if (arc.neighborhood_arc == neighborhood_arc)
                            {
                                return arc.idmgmt_arc;
                            }
                        }
                        error("missing something?");
                    }
                }
                print(@"got a unknown caller:\n");
                print(@"  my_address was $(my_address).\n");
                foreach (HandledNic n in handlednics)
                {
                    string dev = n.dev;
                    print(@"  in $(dev) we have $(n.linklocal).\n");
                }
                return null;
            }
            error(@"not a expected type of caller $(caller.get_type().name()).");
        }

        public IIdentityManagerStub get_stub(IIdmgmtArc arc)
        {
            IdmgmtArc _arc = (IdmgmtArc)arc;
            IAddressManagerStub addrstub = 
                neighborhood_mgr.get_stub_whole_node_unicast(_arc.arc.neighborhood_arc);
            IdentityManagerStubHolder ret = new IdentityManagerStubHolder(addrstub);
            return ret;
        }
    }

    class IdmgmtArc : Object, IIdmgmtArc
    {
        public IdmgmtArc(Arc arc)
        {
            this.arc = arc;
        }
        public weak Arc arc;

        public string get_dev()
        {
            return arc.neighborhood_arc.nic.dev;
        }

        public string get_peer_mac()
        {
            return arc.neighborhood_arc.neighbour_mac;
        }

        public string get_peer_linklocal()
        {
            return arc.neighborhood_arc.neighbour_nic_addr;
        }
    }

    class QspnStubFactory : Object, IQspnStubFactory
    {
        public QspnStubFactory(IdentityData identity_data)
        {
            this.identity_data = identity_data;
        }
        private weak IdentityData identity_data;

        /* This "holder" class is needed because the QspnManagerRemote class provided by
         * the ZCD framework is owned (and tied to) by the AddressManagerXxxxRootStub.
         */
        private class QspnManagerStubHolder : Object, IQspnManagerStub
        {
            public QspnManagerStubHolder(IAddressManagerStub addr, string msg_hdr, IdentityData identity_data)
            {
                this.addr = addr;
                this.msg_hdr = msg_hdr;
                this.identity_data = identity_data;
            }
            private IAddressManagerStub addr;
            private string msg_hdr;
            private weak IdentityData identity_data;

            public IQspnEtpMessage get_full_etp(IQspnAddress requesting_address)
            throws QspnNotAcceptedError, QspnBootstrapInProgressError, StubError, DeserializeError
            {
                string call_id = @"$(get_time_now())";
                print(@"$(call_id): Identity #$(identity_data.local_identity_index): calling RPC get_full_etp: $(msg_hdr).\n");
                print(@"   requesting_address=$(naddr_repr((Naddr)requesting_address)).\n");
                try {
                    IQspnEtpMessage ret = addr.qspn_manager.get_full_etp(requesting_address);
                    print(@"$(get_time_now()): RPC call to get_full_etp sent at $(call_id): returned ret=$(json_string_object(ret)).\n");
                    return ret;
                } catch (QspnNotAcceptedError e) {
                    print(@"$(get_time_now()): RPC call to get_full_etp sent at $(call_id): throwed QspnNotAcceptedError.\n");
                    throw e;
                } catch (QspnBootstrapInProgressError e) {
                    print(@"$(get_time_now()): RPC call to get_full_etp sent at $(call_id): throwed QspnBootstrapInProgressError.\n");
                    throw e;
                } catch (StubError e) {
                    print(@"$(get_time_now()): RPC call to get_full_etp sent at $(call_id): throwed StubError.\n");
                    throw e;
                } catch (DeserializeError e) {
                    print(@"$(get_time_now()): RPC call to get_full_etp sent at $(call_id): throwed DeserializeError.\n");
                    throw e;
                }
            }

            public void got_destroy()
            throws StubError, DeserializeError
            {
                print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): calling RPC got_destroy: $(msg_hdr).\n");
                addr.qspn_manager.got_destroy();
            }

            public void got_prepare_destroy()
            throws StubError, DeserializeError
            {
                print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): calling RPC got_prepare_destroy: $(msg_hdr).\n");
                addr.qspn_manager.got_prepare_destroy();
            }

            public void send_etp(IQspnEtpMessage etp, bool is_full)
            throws QspnNotAcceptedError, StubError, DeserializeError
            {
                string call_id = @"$(get_time_now())";
                print(@"$(call_id): Identity #$(identity_data.local_identity_index): calling RPC send_etp: $(msg_hdr).\n");
                print(@"   etp=$(json_string_object(etp)).\n");
                print(@"   is_full=$(is_full).\n");
                try {
                    addr.qspn_manager.send_etp(etp, is_full);
                    print(@"$(get_time_now()): RPC call to send_etp sent at $(call_id): completed.\n");
                } catch (QspnNotAcceptedError e) {
                    print(@"$(get_time_now()): RPC call to send_etp sent at $(call_id): throwed QspnNotAcceptedError.\n");
                    throw e;
                } catch (StubError e) {
                    print(@"$(get_time_now()): RPC call to send_etp sent at $(call_id): throwed StubError.\n");
                    throw e;
                } catch (DeserializeError e) {
                    print(@"$(get_time_now()): RPC call to send_etp sent at $(call_id): throwed DeserializeError.\n");
                    throw e;
                }
            }
        }

        /* This "void" class is needed for broadcast without arcs.
         */
        private class QspnManagerStubVoid : Object, IQspnManagerStub
        {
            public QspnManagerStubVoid(IdentityData identity_data)
            {
                this.identity_data = identity_data;
            }
            private weak IdentityData identity_data;

            public IQspnEtpMessage get_full_etp(IQspnAddress requesting_address)
            throws QspnNotAcceptedError, QspnBootstrapInProgressError, StubError, DeserializeError
            {
                assert_not_reached();
            }

            public void got_destroy()
            throws StubError, DeserializeError
            {
                print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): would call RPC got_destroy, but have no (other) arcs.\n");
            }

            public void got_prepare_destroy()
            throws StubError, DeserializeError
            {
                print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): would call RPC got_prepare_destroy, but have no (other) arcs.\n");
            }

            public void send_etp(IQspnEtpMessage etp, bool is_full)
            throws QspnNotAcceptedError, StubError, DeserializeError
            {
                print(@"$(get_time_now()): Identity #$(identity_data.local_identity_index): would call RPC send_etp, but have no (other) arcs.\n");
                print(@"   etp=$(json_string_object(etp)).\n");
                print(@"   is_full=$(is_full).\n");
            }
        }

        public IQspnManagerStub
                        i_qspn_get_broadcast(
                            Gee.List<IQspnArc> arcs,
                            IQspnMissingArcHandler? missing_handler=null
                        )
        {
            if(arcs.is_empty) return new QspnManagerStubVoid(identity_data);
            NodeID source_node_id = ((QspnArc)arcs[0]).sourceid;
            ArrayList<NodeID> broadcast_node_id_set = new ArrayList<NodeID>();
            foreach (IQspnArc arc in arcs)
            {
                QspnArc _arc = (QspnArc)arc;
                broadcast_node_id_set.add(_arc.destid);
            }
            INeighborhoodMissingArcHandler? n_missing_handler = null;
            if (missing_handler != null)
            {
                n_missing_handler = new NeighborhoodMissingArcHandler.from_qspn(missing_handler, identity_data);
            }
            IAddressManagerStub addrstub = 
                neighborhood_mgr.get_stub_identity_aware_broadcast(
                source_node_id,
                broadcast_node_id_set,
                n_missing_handler);
            string to_set = ""; foreach (NodeID i in broadcast_node_id_set) to_set += @"$(i.id) ";
            string msg_hdr = @"RPC from $(source_node_id.id) to {$(to_set)}";
            QspnManagerStubHolder ret = new QspnManagerStubHolder(addrstub, msg_hdr, identity_data);
            return ret;
        }

        public IQspnManagerStub
                        i_qspn_get_tcp(
                            IQspnArc arc,
                            bool wait_reply=true
                        )
        {
            QspnArc _arc = (QspnArc)arc;
            IAddressManagerStub addrstub = 
                neighborhood_mgr.get_stub_identity_aware_unicast(
                _arc.arc.neighborhood_arc,
                _arc.sourceid,
                _arc.destid,
                wait_reply);
            string msg_hdr = @"RPC from $(_arc.sourceid.id) to $(_arc.destid.id)";
            QspnManagerStubHolder ret = new QspnManagerStubHolder(addrstub, msg_hdr, identity_data);
            return ret;
        }
    }

    class ThresholdCalculator : Object, IQspnThresholdCalculator
    {
        public int i_qspn_calculate_threshold(IQspnNodePath p1, IQspnNodePath p2)
        {
            return 10000;
        }
    }

    class QspnArc : Object, IQspnArc
    {
        public QspnArc(Arc arc, NodeID sourceid, NodeID destid, IdentityArc ia, string peer_mac)
        {
            this.arc = arc;
            this.sourceid = sourceid;
            this.destid = destid;
            this.ia = ia;
            this.peer_mac = peer_mac;
            cost_seed = Random.int_range(0, 1000);
        }
        public weak Arc arc;
        public NodeID sourceid;
        public NodeID destid;
        public weak IdentityArc ia;
        public string peer_mac;
        private int cost_seed;

        public IQspnCost i_qspn_get_cost()
        {
            return new Cost(arc.cost + cost_seed);
        }

        public bool i_qspn_equals(IQspnArc other)
        {
            return other == this;
        }

        public bool i_qspn_comes_from(CallerInfo rpc_caller)
        {
            string neighbour_nic_addr = arc.neighborhood_arc.neighbour_nic_addr;
            if (rpc_caller is TcpclientCallerInfo)
            {
                return neighbour_nic_addr == ((TcpclientCallerInfo)rpc_caller).peer_address;
            }
            else if (rpc_caller is BroadcastCallerInfo)
            {
                return neighbour_nic_addr == ((BroadcastCallerInfo)rpc_caller).peer_address;
            }
            else if (rpc_caller is UnicastCallerInfo)
            {
                warning("QspnArc.i_qspn_comes_from: got a call in udp-unicast. Ignore it.");
                tasklet.exit_tasklet(null);
            }
            else
            {
                assert_not_reached();
            }
        }
    }

    class ServerDelegate : Object, IRpcDelegate
    {
        public Gee.List<IAddressManagerSkeleton> get_addr_set(CallerInfo caller)
        {
            if (caller is TcpclientCallerInfo)
            {
                TcpclientCallerInfo c = (TcpclientCallerInfo)caller;
                string peer_address = c.peer_address;
                ISourceID sourceid = c.sourceid;
                IUnicastID unicastid = c.unicastid;
                var ret = new ArrayList<IAddressManagerSkeleton>();
                IAddressManagerSkeleton? d = neighborhood_mgr.get_dispatcher(sourceid, unicastid, peer_address, null);
                if (d != null) ret.add(d);
                return ret;
            }
            else if (caller is UnicastCallerInfo)
            {
                UnicastCallerInfo c = (UnicastCallerInfo)caller;
                string peer_address = c.peer_address;
                string dev = c.dev;
                ISourceID sourceid = c.sourceid;
                IUnicastID unicastid = c.unicastid;
                var ret = new ArrayList<IAddressManagerSkeleton>();
                IAddressManagerSkeleton? d = neighborhood_mgr.get_dispatcher(sourceid, unicastid, peer_address, dev);
                if (d != null) ret.add(d);
                return ret;
            }
            else if (caller is BroadcastCallerInfo)
            {
                BroadcastCallerInfo c = (BroadcastCallerInfo)caller;
                string peer_address = c.peer_address;
                string dev = c.dev;
                ISourceID sourceid = c.sourceid;
                IBroadcastID broadcastid = c.broadcastid;
                return neighborhood_mgr.get_dispatcher_set(sourceid, broadcastid, peer_address, dev);
            }
            else
            {
                error(@"Unexpected class $(caller.get_type().name())");
            }
        }
    }

    class ServerErrorHandler : Object, IRpcErrorHandler
    {
        public void error_handler(Error e)
        {
            error(@"error_handler: $(e.message)");
        }
    }

    class AddressManagerForIdentity : Object, IAddressManagerSkeleton
    {
        public AddressManagerForIdentity(IQspnManagerSkeleton qspn_mgr)
        {
            this.qspn_mgr = qspn_mgr;
        }
        private weak IQspnManagerSkeleton qspn_mgr;

        public unowned INeighborhoodManagerSkeleton
        neighborhood_manager_getter()
        {
            warning("AddressManagerForIdentity.neighborhood_manager_getter: not for identity");
            tasklet.exit_tasklet(null);
        }

        protected unowned IIdentityManagerSkeleton
        identity_manager_getter()
        {
            warning("AddressManagerForIdentity.identity_manager_getter: not for identity");
            tasklet.exit_tasklet(null);
        }

        public unowned IQspnManagerSkeleton
        qspn_manager_getter()
        {
            return qspn_mgr;
        }

        public unowned IPeersManagerSkeleton
        peers_manager_getter()
        {
            error("not in this test");
        }

        public unowned ICoordinatorManagerSkeleton
        coordinator_manager_getter()
        {
            error("not in this test");
        }
    }

    class AddressManagerForNode : Object, IAddressManagerSkeleton
    {
        public weak INeighborhoodManagerSkeleton neighborhood_mgr;
        public weak IIdentityManagerSkeleton identity_mgr;

        public unowned INeighborhoodManagerSkeleton
        neighborhood_manager_getter()
        {
            return neighborhood_mgr;
        }

        protected unowned IIdentityManagerSkeleton
        identity_manager_getter()
        {
            return identity_mgr;
        }

        public unowned IQspnManagerSkeleton
        qspn_manager_getter()
        {
            warning("AddressManagerForNode.qspn_manager_getter: not for node");
            tasklet.exit_tasklet(null);
        }

        public unowned IPeersManagerSkeleton
        peers_manager_getter()
        {
            error("not in this test");
        }

        public unowned ICoordinatorManagerSkeleton
        coordinator_manager_getter()
        {
            error("not in this test");
        }
    }
}

