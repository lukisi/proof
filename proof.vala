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
    int identityarc_nextindex;
    HashMap<int, IdentityArc> identityarcs;

    IdentityData find_or_create_local_identity(NodeID node_id, out int local_identity_index)
    {
        foreach (int k in local_identities.keys)
        {
            NodeID local_nodeid = local_identities[k].nodeid;
            if (local_nodeid.equals(node_id))
            {
                local_identity_index = k;
                return local_identities[k];
            }
        }
        local_identity_index = local_identity_nextindex++;
        IdentityData ret = new IdentityData(node_id);
        local_identities[local_identity_index] = ret;
        ret.local_identity_index = local_identity_index;
        return ret;
    }

    IdentityArc find_identity_arc(IIdmgmtIdentityArc id_arc)
    {
        foreach (int k in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[k];
            if (ia.id_arc == id_arc) return ia;
        }
        error("IdentityArc not found in identityarcs");
    }

    AddressManagerForNode node_skeleton;
    ServerDelegate dlg;
    ServerErrorHandler err;
    ArrayList<ITaskletHandle> t_udp_list;

    const string pipe_response = "/tmp/qspnclient_response";
    const string pipe_commands = "/tmp/qspnclient_commands";
    int server_fd_commands;
    int client_fd_response;

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
                print("""
Command list:

> show_local_identities
  List current local identities.

> show_neighborhood_arcs
  List current usable arcs.

> add_real_arc <from_MAC> <to_MAC> <cost>
  Use the specific arc.
  You choose a cost in microseconds for it.

> show_real_arcs
  List current accepted arcs.

> change_real_arc <from_MAC> <to_MAC> <cost>
  Change the cost (in microsecond) for a given arc, which was already accepted.

> remove_real_arc <from_MAC> <to_MAC>
  Remove a given arc, which was already accepted.

> show_identityarcs
  List current identity-arcs.

> prepare_enter_net_phase_1
  Prepare ...

> enter_net_phase_1
  Finalize ...

> add_qspnarc <local_identity_index> <identityarc_index>
  Add a QspnArc.

> check_connectivity <local_identity_index>
  Checks whether a connectivity identity is still necessary.

> help
  Show this menu.

> quit
  Exit.

""");

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
        ArrayList<int> _elderships = new ArrayList<int>();
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
        for (int i = 0; i < _gsizes.size; i++) _elderships.add(0);
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
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {"sysctl", "net.ipv4.ip_forward=1"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {"sysctl", "net.ipv4.conf.all.rp_filter=0"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {"ip", "address", "add", ntklocalhost, "dev", "lo"}));
        cm.end_block(bid);

        real_nics = new ArrayList<string>();
        handlednics = new ArrayList<HandledNic>();
        local_identity_nextindex = 0;
        local_identities = new HashMap<int, IdentityData>();

        neighborhood_arcs = new HashMap<string, INeighborhoodArc>();
        real_arcs = new HashMap<string, Arc>();
        identityarc_nextindex = 0;
        identityarcs = new HashMap<int, IdentityArc>();
        pending_prepared_enter_net_operations = new HashMap<string,PreparedEnterNet>();

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
        neighborhood_mgr.nic_address_set.connect(nic_address_set);
        neighborhood_mgr.arc_added.connect(arc_added);
        neighborhood_mgr.arc_changed.connect(arc_changed);
        neighborhood_mgr.arc_removing.connect(arc_removing);
        neighborhood_mgr.arc_removed.connect(arc_removed);
        neighborhood_mgr.nic_address_unset.connect(nic_address_unset);
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
        identity_mgr.identity_arc_added.connect(identity_arc_added);
        identity_mgr.identity_arc_changed.connect(identity_arc_changed);
        identity_mgr.identity_arc_removing.connect(identity_arc_removing);
        identity_mgr.identity_arc_removed.connect(identity_arc_removed);
        identity_mgr.arc_removed.connect(identity_mgr_arc_removed);

        // First identity
        cm.single_command(new ArrayList<string>.wrap({
            @"ip", @"rule", @"add", @"table", @"ntk"}));

        NodeID nodeid = identity_mgr.get_main_id();
        int local_identity_index;
        IdentityData first_identity_data = find_or_create_local_identity(nodeid, out local_identity_index);
        // First qspn manager
        QspnManager.init(tasklet, max_paths, max_common_hops_ratio, arc_timeout, new ThresholdCalculator());
        Naddr my_naddr = new Naddr(_naddr.to_array(), _gsizes.to_array());
        Fingerprint my_fp = new Fingerprint(_elderships.to_array());
        QspnManager qspn_mgr = new QspnManager.create_net(my_naddr,
            my_fp,
            new QspnStubFactory(local_identity_index));
        identity_mgr.set_identity_module(nodeid, "qspn", qspn_mgr);
        first_identity_data.my_naddr = my_naddr;
        first_identity_data.my_fp = my_fp;
        first_identity_data.ready = true;
        first_identity_data.addr_man = new AddressManagerForIdentity(qspn_mgr);

        foreach (string s in print_local_identity(0)) print(s + "\n");

        first_identity_data.local_ip_set.global = ip_global_node(my_naddr.pos);
        foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.global)", @"dev", @"$dev"}));
        first_identity_data.local_ip_set.anonymous = ip_anonymizing_node(my_naddr.pos);
        if (accept_anonymous_requests)
        {
            foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.anonymous)", @"dev", @"$dev"}));
        }
        for (int i = levels-1; i >= 1; i--)
        {
            first_identity_data.local_ip_set.intern[i] = ip_internal_node(my_naddr.pos, i);
            foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"add", @"$(first_identity_data.local_ip_set.intern[i])", @"dev", @"$dev"}));
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
                    @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"ntk"}));
                ipaddr = first_identity_data.destination_ip_set[i][j].anonymous;
                cm.single_command_in_block(bid, new ArrayList<string>.wrap({
                    @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"ntk"}));
            }
            for (int k = levels-1; k >= i+1; k--)
            {
                if (first_identity_data.destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = first_identity_data.destination_ip_set[i][j].intern[k];
                    cm.single_command_in_block(bid, new ArrayList<string>.wrap({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"ntk"}));
                }
            }
        }
        cm.end_block(bid);

        if (! no_anonymize)
        {
            string anonymousrange = ip_anonymizing_gnode(_naddr, levels);
            cm.single_command(new ArrayList<string>.wrap({
                @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$anonymousrange",
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
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$range2",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$range3", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                }
                else
                {
                    string range2 = ip_global_gnode(_naddr, subnetlevel);
                    string range3 = ip_global_gnode(_naddr, levels);
                    string range4 = ip_anonymizing_gnode(_naddr, subnetlevel);
                    string range5 = ip_anonymizing_gnode(_naddr, levels);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$range2",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$range3", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                    if (accept_anonymous_requests) cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"PREROUTING", @"-d", @"$range4",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$range5", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                }
            }
        }

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

        // end startup

        // start a tasklet to get commands from pipe_commands.
        ReadCommandsTasklet ts = new ReadCommandsTasklet();
        ITaskletHandle h_read_commands = tasklet.spawn(ts);

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

        // TODO cleanup

        // Remove identities and their network namespaces and linklocal addresses.
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

        assert(local_identities.size == 1);
        int kk = -1;
        foreach (int k in local_identities.keys) kk = k;
        IdentityData identity_data = local_identities[kk];
        assert(identity_data.main_id);
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
                @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$anonymousrange",
                @"-j", @"SNAT", @"--to", @"$(identity_data.local_ip_set.global)"}));
        }

        // remove local addresses (global, anon, intern, localhost)
        if (identity_data.local_ip_set.global != "")
            foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"del", @"$(identity_data.local_ip_set.global)/32", @"dev", @"$dev"}));
        if (identity_data.local_ip_set.anonymous != "" && accept_anonymous_requests)
            foreach (string dev in real_nics)
            cm.single_command(new ArrayList<string>.wrap({
                @"ip", @"address", @"del", @"$(identity_data.local_ip_set.anonymous)/32", @"dev", @"$dev"}));
        for (int i = levels-1; i >= 1; i--)
        {
            if (identity_data.local_ip_set.intern[i] != "")
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(identity_data.local_ip_set.intern[i])/32", @"dev", @"$dev"}));
        }
        cm.single_command(new ArrayList<string>.wrap(
            {"ip", "address", "del", @"$(ntklocalhost)/32", "dev", "lo"}));

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
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$range2",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$range3", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                }
                else
                {
                    string range2 = ip_global_gnode(_naddr, subnetlevel);
                    string range3 = ip_global_gnode(_naddr, levels);
                    string range4 = ip_anonymizing_gnode(_naddr, subnetlevel);
                    string range5 = ip_anonymizing_gnode(_naddr, levels);
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$range2",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$range3", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                    if (accept_anonymous_requests) cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"PREROUTING", @"-d", @"$range4",
                        @"-j", @"NETMAP", @"--to", @"$range1"}));
                    cm.single_command(new ArrayList<string>.wrap({
                        @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$range5", @"-s", @"$range1",
                        @"-j", @"NETMAP", @"--to", @"$range2"}));
                }
            }
        }

        // First, we call stop_monitor_all of NeighborhoodManager.
        neighborhood_mgr.stop_monitor_all();
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

    size_t nonblock_read(int fd, void* b, size_t nbytes) throws Error
    {
        ssize_t result = Posix.read(fd, b, nbytes);
        if (result == -1)
        {
            if (errno == Posix.EAGAIN) return (size_t)0;
            report_error_posix_read();
        }
        return (size_t)result;
    }

    [NoReturn]
    void report_error_posix_read() throws Error
    {
        if (errno == Posix.EAGAIN)
            throw new FileError.FAILED(@"Posix.read returned EAGAIN");
        if (errno == Posix.EWOULDBLOCK)
            throw new FileError.FAILED(@"Posix.read returned EWOULDBLOCK");
        if (errno == Posix.EBADF)
            throw new FileError.FAILED(@"Posix.read returned EBADF");
        if (errno == Posix.ECONNREFUSED)
            throw new FileError.FAILED(@"Posix.read returned ECONNREFUSED");
        if (errno == Posix.EFAULT)
            throw new FileError.FAILED(@"Posix.read returned EFAULT");
        if (errno == Posix.EINTR)
            throw new FileError.FAILED(@"Posix.read returned EINTR");
        if (errno == Posix.EINVAL)
            throw new FileError.FAILED(@"Posix.read returned EINVAL");
        if (errno == Posix.ENOMEM)
            throw new FileError.FAILED(@"Posix.read returned ENOMEM");
        if (errno == Posix.ENOTCONN)
            throw new FileError.FAILED(@"Posix.read returned ENOTCONN");
        if (errno == Posix.ENOTSOCK)
            throw new FileError.FAILED(@"Posix.read returned ENOTSOCK");
        throw new FileError.FAILED(@"Posix.read returned -1, errno = $(errno)");
    }

    void server_open_pipe_commands()
    {
        int ret = Posix.mkfifo(pipe_commands, Posix.S_IRUSR | Posix.S_IWUSR);
        if (ret != 0 && Posix.errno == Posix.EEXIST)
        {
            error("Server is already in progress.");
        }
        if (ret != 0) error(@"Couldn't create pipe commands: Posix.errno = $(Posix.errno)");
        server_fd_commands = Posix.open(pipe_commands, Posix.O_RDONLY | Posix.O_NONBLOCK);
        if (server_fd_commands == -1) error(@"Couldn't open pipe commands: Posix.errno = $(Posix.errno)");
    }

    void client_open_pipe_response()
    {
        int ret = Posix.mkfifo(pipe_response, Posix.S_IRUSR | Posix.S_IWUSR);
        if (ret != 0 && Posix.errno == Posix.EEXIST)
        {
            error("Client: Another command is now in progress.");
        }
        if (ret != 0) error(@"Couldn't create pipe response: Posix.errno = $(Posix.errno)");
        client_fd_response = Posix.open(pipe_response, Posix.O_RDONLY | Posix.O_NONBLOCK);
        if (client_fd_response == -1) error(@"Couldn't open pipe response: Posix.errno = $(Posix.errno)");
    }

    void remove_pipe_commands()
    {
        Posix.close(server_fd_commands);
        Posix.unlink(pipe_commands);
    }

    void remove_pipe_response()
    {
        Posix.close(client_fd_response);
        Posix.unlink(pipe_response);
    }

    bool check_pipe_response()
    {
        return check_pipe(pipe_response);
    }

    bool check_pipe(string fname)
    {
        Posix.Stat sb;
        int ret = Posix.stat(fname, out sb);
        if (ret != 0 && Posix.errno == Posix.ENOENT) return false;
        if (ret != 0)
        {
            print(@"check_pipe($(fname)): stat: ret = $(ret)\n");
            print(@"stat: errno = $(Posix.errno)\n");
            switch (Posix.errno)
            {
                case Posix.EACCES:
                    print("EACCES\n");
                    break;
                case Posix.EBADF:
                    print("EBADF\n");
                    break;
                case Posix.EFAULT:
                    print("EFAULT\n");
                    break;
                case Posix.ELOOP:
                    print("ELOOP\n");
                    break;
                case Posix.ENAMETOOLONG:
                    print("ENAMETOOLONG\n");
                    break;
                default:
                    print("???\n");
                    break;
            }
            error(@"unexpected stat retcode");
        }
        if (Posix.S_ISFIFO(sb.st_mode)) return true;
        error(@"unexpected stat result from file $(fname)");
    }

    string read_command() throws Error
    {
        uint8 buf[256];
        size_t len = 0;
        while (true)
        {
            while (true)
            {
                size_t nb = nonblock_read(server_fd_commands, (void*)(((uint8*)buf)+len), 1);
                if (nb == 0)
                {
                    tasklet.ms_wait(2);
                }
                else
                {
                    len += nb;
                    break;
                }
            }
            if (buf[len-1] == '\n') break;
            if (len >= buf.length) error("command too long");
        }
        string line = (string)buf;
        line = line.substring(0, line.length-1);
        return line;
    }

    void write_response(string _res) throws Error
    {
        string res = _res + "\n";
        int fd_response = Posix.open(pipe_response, Posix.O_WRONLY);
        size_t remaining = res.length;
        uint8 *buf = res.data;
        while (remaining > 0)
        {
            size_t len = tasklet.write(fd_response, (void*)buf, remaining);
            remaining -= len;
            buf += len;
        }
        Posix.close(fd_response);
    }

    void write_block_response(string command_id, Gee.List<string> lines, int retval=0) throws Error
    {
        write_response(@"$(command_id) $(retval) $(lines.size)");
        foreach (string line in lines) write_response(line);
    }

    void write_empty_response(string command_id, int retval=0) throws Error
    {
        write_block_response(command_id, new ArrayList<string>(), retval);
    }

    void write_oneline_response(string command_id, string line, int retval=0) throws Error
    {
        write_block_response(command_id, new ArrayList<string>.wrap({line}), retval);
    }

    void write_command(string _res) throws Error
    {
        string res = _res + "\n";
        int fd_commands = Posix.open(pipe_commands, Posix.O_WRONLY);
        size_t remaining = res.length;
        uint8 *buf = res.data;
        while (remaining > 0)
        {
            size_t len = tasklet.write(fd_commands, (void*)buf, remaining);
            remaining -= len;
            buf += len;
        }
        Posix.close(fd_commands);
    }

    string read_response() throws Error
    {
        uint8 buf[256];
        size_t len = 0;
        while (true)
        {
            while (true)
            {
                size_t nb = nonblock_read(client_fd_response, (void*)(((uint8*)buf)+len), 1);
                if (nb == 0)
                {
                    tasklet.ms_wait(2);
                }
                else
                {
                    len += nb;
                    break;
                }
            }
            if (buf[len-1] == '\n') break;
            if (len >= buf.length) error("response too long");
        }
        string line = (string)buf;
        line = line.substring(0, line.length-1);
        return line;
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
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {@"sysctl", @"net.ipv4.conf.$(dev).rp_filter=0"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {@"sysctl", @"net.ipv4.conf.$(dev).arp_ignore=1"}));
        cm.single_command_in_block(bid, new ArrayList<string>.wrap(
            {@"sysctl", @"net.ipv4.conf.$(dev).arp_announce=2"}));
        cm.end_block(bid);

        // Start listen UDP on dev
        t_udp_list.add(udp_listen(dlg, err, ntkd_port, dev));
        // Run monitor
        neighborhood_mgr.start_monitor(new NeighborhoodNetworkInterface(dev));
    }

    class ReadCommandsTasklet : Object, ITaskletSpawnable
    {
        void handle_commands()
        {
            try
            {
                while (true)
                {
                    string line = read_command();
                    if (! check_pipe_response())
                    {
                        print("Server: ignore command because no pipe for response.\n");
                        continue;
                    }
                    assert(line != "");
                    ArrayList<string> _args = new ArrayList<string>();
                    foreach (string s_piece in line.split(" ")) _args.add(s_piece);
                    string command_id = _args.remove_at(0);
                    assert(_args.size > 0);
                    if (_args[0] == "quit")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_empty_response(command_id);
                        tasklet.ms_wait(5);
                        do_me_exit = true;
                        break;
                    }
                    else if (_args[0] == "show_handlednics")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_block_response(command_id, show_handlednics());
                    }
                    else if (_args[0] == "show_local_identities")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_block_response(command_id, show_local_identities());
                    }
                    else if (_args[0] == "show_neighborhood_arcs")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_block_response(command_id, show_neighborhood_arcs());
                    }
                    else if (_args[0] == "add_real_arc")
                    {
                        if (_args.size != 4)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        string k = key_for_physical_arc(_args[1], _args[2]);
                        int i_cost = int.parse(_args[3]);
                        if (! (k in neighborhood_arcs.keys))
                        {
                            write_oneline_response(command_id, @"wrong MAC pair '$(k)'", 1);
                            continue;
                        }
                        write_block_response(command_id, add_real_arc(k, i_cost));
                    }
                    else if (_args[0] == "show_real_arcs")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_block_response(command_id, show_real_arcs());
                    }
                    else if (_args[0] == "change_real_arc")
                    {
                        if (_args.size != 4)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        string k = key_for_physical_arc(_args[1], _args[2]);
                        int i_cost = int.parse(_args[3]);
                        if (! (k in real_arcs.keys))
                        {
                            write_oneline_response(command_id, @"wrong MAC pair '$(k)'", 1);
                            continue;
                        }
                        change_real_arc(k, i_cost);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "remove_real_arc")
                    {
                        if (_args.size != 3)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        string k = key_for_physical_arc(_args[1], _args[2]);
                        if (! (k in real_arcs.keys))
                        {
                            write_oneline_response(command_id, @"wrong MAC pair '$(k)'", 1);
                            continue;
                        }
                        remove_real_arc(k);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "show_identityarcs")
                    {
                        if (_args.size != 1)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        write_block_response(command_id, show_identityarcs());
                    }
                    else if (_args[0] == "prepare_enter_net_phase_1")
                    {
                        if (_args.size != 15)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        int guest_gnode_level = int.parse(_args[2]);
                        int host_gnode_level = int.parse(_args[3]);
                        string host_gnode_address = _args[4];
                        string host_gnode_data = _args[5];
                        int in_host_pos1 = int.parse(_args[6]);
                        int in_host_pos1_eldership = int.parse(_args[7]);
                        int in_host_pos2 = int.parse(_args[8]);
                        int in_host_pos2_eldership = int.parse(_args[9]);
                        int connectivity_pos = int.parse(_args[10]);
                        int connectivity_pos_eldership = int.parse(_args[11]);
                        string id_arc_index_list_str = _args[12];
                        assert(id_arc_index_list_str.has_prefix("["));
                        assert(id_arc_index_list_str.has_suffix("]"));
                        id_arc_index_list_str = id_arc_index_list_str.substring(1, id_arc_index_list_str.length-2);
                        ArrayList<string> id_arc_index_list = new ArrayList<string>();
                        foreach (string s_piece in id_arc_index_list_str.split(",")) id_arc_index_list.add(s_piece);
                        int op_id = int.parse(_args[13]);
                        string prev_op_id_str = _args[14];
                        int? prev_op_id = null;
                        if (prev_op_id_str != "null") prev_op_id = int.parse(prev_op_id_str);
                        prepare_enter_net_phase_1(
                            local_identity_index,
                            guest_gnode_level,
                            host_gnode_level,
                            host_gnode_address,
                            host_gnode_data,
                            in_host_pos1,
                            in_host_pos1_eldership,
                            in_host_pos2,
                            in_host_pos2_eldership,
                            connectivity_pos,
                            connectivity_pos_eldership,
                            id_arc_index_list,
                            op_id,
                            prev_op_id);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "enter_net_phase_1")
                    {
                        if (_args.size != 3)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        int op_id = int.parse(_args[2]);
                        enter_net_phase_1(
                            local_identity_index,
                            op_id);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "add_qspnarc")
                    {
                        if (_args.size != 3)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        int idarc_index = int.parse(_args[2]);
                        add_qspnarc(local_identity_index, idarc_index);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "check_connectivity")
                    {
                        if (_args.size != 2)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        if (! (local_identity_index in local_identities.keys))
                        {
                            write_oneline_response(command_id, @"wrong local_identity_index '$(local_identity_index)'", 1);
                            continue;
                        }
                        write_block_response(command_id, check_connectivity(local_identity_index));
                    }
                    else
                    {
                        write_oneline_response(command_id, @"unknown command '$(_args[0])'.", 1);
                    }
                }
            } catch (Error e) {
                remove_pipe_commands();
                error(@"Error during pass of command or response: $(e.message)");
            }
        }

        public void * func()
        {
            handle_commands();
            return null;
        }
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
        public string peer_mac;
        public string peer_linklocal;
        public QspnArc? qspn_arc;
        public IdentityArc(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc, string peer_mac, string peer_linklocal)
        {
            this.arc = arc;
            this.id = id;
            this.id_arc = id_arc;
            this.peer_mac = peer_mac;
            this.peer_linklocal = peer_linklocal;
            qspn_arc = null;
        }
    }

    class NeighborData : Object
    {
        public string mac;
        public HCoord h;
    }

    class BestRoute : Object
    {
        public string gw;
        public string dev;
    }

    class LocalIPSet : Object
    {
        public string global;
        public string anonymous;
        public HashMap<int,string> intern;
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
            ready = false;
            my_identityarcs = new ArrayList<IdentityArc>();
            connectivity_from_level = 0;
            connectivity_to_level = 0;
            copy_of_identity = null;

            local_ip_set = new LocalIPSet();
            local_ip_set.global = "";
            local_ip_set.anonymous = "";
            local_ip_set.intern = new HashMap<int,string>();
            for (int j = 1; j < levels; j++) local_ip_set.intern[j] = "";

            destination_ip_set = init_destination_ip_set();
        }

        public NodeID nodeid;
        public Naddr my_naddr;
        public Fingerprint my_fp;

        private string _network_namespace;
        public string network_namespace {
            get {
                _network_namespace = identity_mgr.get_namespace(nodeid);
                return _network_namespace;
            }
        }

        public IdentityData? copy_of_identity;
        public int local_identity_index;
        public bool ready;
        public AddressManagerForIdentity addr_man;
        public ArrayList<IdentityArc> my_identityarcs;
        public int connectivity_from_level;
        public int connectivity_to_level;

        public LocalIPSet local_ip_set;
        public HashMap<int,HashMap<int,DestinationIPSet>> destination_ip_set;

        public string ip_global;
        public ArrayList<string> ip_internal;

        public NetworkStack network_stack {
            get {
                error("do not use network_stack");
            }
        }

        public bool main_id {
            get {
                return network_namespace == "";
            }
        }

        // handle signals from qspn_manager

        public void arc_removed(IQspnArc arc, bool bad_link)
        {
            error("not implemented yet");
            /*
            QspnArc _arc = (QspnArc)arc;
            my_arcs.remove(_arc);
            network_stack.remove_neighbour(_arc.peer_mac);
            if (bad_link)
            {
                // Remove arc from neighborhood, because it fails.
                neighborhood_mgr.remove_my_arc(_arc.arc.neighborhood_arc, false);
            }
            else
            {
                identity_mgr.remove_identity_arc(_arc.arc.idmgmt_arc, _arc.sourceid, _arc.destid, true);
            }
            */
        }

        public void changed_fp(int l)
        {
            // TODO
        }

        public void changed_nodes_inside(int l)
        {
            // TODO
        }

        public void destination_added(HCoord h)
        {
            // something to do?
        }

        public void destination_removed(HCoord h)
        {
            // something to do?
        }

        public void gnode_splitted(IQspnArc a, HCoord d, IQspnFingerprint fp)
        {
            // TODO
            // we should do something of course
            error("not implemented yet");
        }

        public void path_added(IQspnNodePath p)
        {
            update_best_path(p.i_qspn_get_hops().last().i_qspn_get_hcoord());
        }

        public void path_changed(IQspnNodePath p)
        {
            update_best_path(p.i_qspn_get_hops().last().i_qspn_get_hcoord());
        }

        public void path_removed(IQspnNodePath p)
        {
            update_best_path(p.i_qspn_get_hops().last().i_qspn_get_hcoord());
        }

        private void update_best_path(HCoord h)
        {
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
            if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.
            print(@"Debug: IdentityData #$(local_identity_index): update_best_path for h ($(h.lvl), $(h.pos)): started.\n");
            // change the route. place current best path to `h`. if none, then change the path to 'unreachable'.

            // Retrieve all routes towards `h`.
            Gee.List<IQspnNodePath> paths;
            try {
                paths = qspn_mgr.get_paths_to(h);
            } catch (QspnBootstrapInProgressError e) {
                paths = new ArrayList<IQspnNodePath>();
            }

            // Compute Netsukuku address of `h`.
            ArrayList<int> h_addr = new ArrayList<int>();
            h_addr.add_all(my_naddr.pos);
            h_addr[h.lvl] = h.pos;
            for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

            // Compute list of neighbors.
            ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
            foreach (IdentityArc ia in my_identityarcs) if (ia.qspn_arc != null)
            {
                Arc arc = ((IdmgmtArc)ia.arc).arc;
                IQspnNaddr? _neighbour_naddr = qspn_mgr.get_naddr_for_arc(ia.qspn_arc);
                if (_neighbour_naddr == null) continue;
                Naddr neighbour_naddr = (Naddr)_neighbour_naddr;
                INeighborhoodArc neighborhood_arc = arc.neighborhood_arc;
                NeighborData neighbor = new NeighborData();
                neighbor.mac = neighborhood_arc.neighbour_mac;
                neighbor.h = my_naddr.i_qspn_get_coord_by_address(neighbour_naddr);
                neighbors.add(neighbor);
            }

            // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
            HashMap<string, BestRoute> best_routes = find_best_routes(paths, neighbors);

            // Operations now are based on type of my_naddr:
            // Is this the main ID? Do I have a *real* Netsukuku address?
            int real_up_to = my_naddr.get_real_up_to();
            int virtual_up_to = my_naddr.get_virtual_up_to();
            if (main_id)
            {
                if (real_up_to == levels-1)
                {
                    // Compute IP dest addresses and src addresses.
                    ArrayList<string> ip_dest_set = new ArrayList<string>();
                    ArrayList<string> ip_src_set = new ArrayList<string>();
                    // Global.
                    ip_dest_set.add(ip_global_gnode(h_addr, h.lvl));
                    ip_src_set.add(ip_global);
                    // Anonymizing.
                    ip_dest_set.add(ip_anonymizing_gnode(h_addr, h.lvl));
                    ip_src_set.add(ip_global);
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                        ip_src_set.add(ip_internal[t-1]);
                    }

                    for (int i = 0; i < ip_dest_set.size; i++)
                    {
                        string d_x = ip_dest_set[i];
                        string n_x = ip_src_set[i];
                        // For packets in egress:
                        if (best_routes.has_key("main"))
                            network_stack.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    n_x,
                                    null);
                        else network_stack.change_best_path(d_x, null, null, null, null);
                        // For packets in forward, received from a known MAC:
                        foreach (NeighborData neighbor in neighbors)
                        {
                            if (best_routes.has_key(neighbor.mac))
                            {
                                network_stack.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                            }
                        }
                        // For packets in forward, received from a unknown MAC:
                        /* No need because the system uses the same as per packets in egress */
                    }
                }
                else
                {
                    if (h.lvl <= real_up_to)
                    {
                        // Compute IP dest addresses and src addresses.
                        ArrayList<string> ip_dest_set = new ArrayList<string>();
                        ArrayList<string> ip_src_set = new ArrayList<string>();
                        // Internals. In this case they MUST be checked.
                        bool invalid_found = false;
                        for (int t = h.lvl + 1; t <= levels - 1; t++)
                        {
                            for (int n_lvl = h.lvl + 1; n_lvl <= t - 1; n_lvl++)
                            {
                                if (h_addr[n_lvl] >= _gsizes[n_lvl])
                                {
                                    invalid_found = true;
                                    break;
                                }
                            }
                            if (invalid_found) break; // The higher levels will be invalid too.
                            ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                            ip_src_set.add(ip_internal[t-1]);
                        }

                        for (int i = 0; i < ip_dest_set.size; i++)
                        {
                            string d_x = ip_dest_set[i];
                            string n_x = ip_src_set[i];
                            // For packets in egress:
                            if (best_routes.has_key("main"))
                                network_stack.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        n_x,
                                        null);
                            else network_stack.change_best_path(d_x, null, null, null, null);
                            // For packets in forward, received from a known MAC:
                            foreach (NeighborData neighbor in neighbors)
                            {
                                if (best_routes.has_key(neighbor.mac))
                                {
                                    network_stack.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                                }
                            }
                            // For packets in forward, received from a unknown MAC:
                            /* No need because the system uses the same as per packets in egress */
                        }
                    }
                    else if (h.lvl < virtual_up_to)
                    {
                        // Compute IP dest addresses (in this case no src addresses).
                        ArrayList<string> ip_dest_set = new ArrayList<string>();
                        // Internals. In this case they MUST be checked.
                        bool invalid_found = false;
                        for (int t = h.lvl + 1; t <= levels - 1; t++)
                        {
                            for (int n_lvl = h.lvl + 1; n_lvl <= t - 1; n_lvl++)
                            {
                                if (h_addr[n_lvl] >= _gsizes[n_lvl])
                                {
                                    invalid_found = true;
                                    break;
                                }
                            }
                            if (invalid_found) break; // The higher levels will be invalid too.
                            ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                        }

                        for (int i = 0; i < ip_dest_set.size; i++)
                        {
                            string d_x = ip_dest_set[i];

                            // For packets in egress:
                            /* Nothing: We are the main identity, but we don't have a valid src IP at this level. */
                            // For packets in forward, received from a known MAC:
                            foreach (NeighborData neighbor in neighbors)
                            {
                                if (best_routes.has_key(neighbor.mac))
                                {
                                    network_stack.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                                }
                            }
                            // For packets in forward, received from a unknown MAC:
                            if (best_routes.has_key("main"))
                                network_stack.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        null,
                                        null);
                            else network_stack.change_best_path(d_x, null, null, null, null);
                        }
                    }
                    else
                    {
                        // Compute IP dest addresses (in this case no src addresses).
                        ArrayList<string> ip_dest_set = new ArrayList<string>();
                        // Global.
                        ip_dest_set.add(ip_global_gnode(h_addr, h.lvl));
                        // Anonymizing.
                        ip_dest_set.add(ip_anonymizing_gnode(h_addr, h.lvl));
                        // Internals. In this case they are guaranteed to be valid.
                        for (int t = h.lvl + 1; t <= levels - 1; t++)
                        {
                            ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                        }

                        for (int i = 0; i < ip_dest_set.size; i++)
                        {
                            string d_x = ip_dest_set[i];

                            // For packets in egress:
                            /* Nothing: We are the main identity, but we don't have a valid src IP at this level. */
                            // For packets in forward, received from a known MAC:
                            foreach (NeighborData neighbor in neighbors)
                            {
                                if (best_routes.has_key(neighbor.mac))
                                {
                                    network_stack.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                                }
                            }
                            // For packets in forward, received from a unknown MAC:
                            if (best_routes.has_key("main"))
                                network_stack.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        null,
                                        null);
                            else network_stack.change_best_path(d_x, null, null, null, null);
                        }
                    }
                }
            }
            else
            {
                if (h.lvl < virtual_up_to)
                {
                    // Compute IP dest addresses (in this case no src addresses).
                    ArrayList<string> ip_dest_set = new ArrayList<string>();
                    // Internals. In this case they MUST be checked.
                    bool invalid_found = false;
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        for (int n_lvl = h.lvl + 1; n_lvl <= t - 1; n_lvl++)
                        {
                            if (h_addr[n_lvl] >= _gsizes[n_lvl])
                            {
                                invalid_found = true;
                                break;
                            }
                        }
                        if (invalid_found) break; // The higher levels will be invalid too.
                        ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                    }

                    for (int i = 0; i < ip_dest_set.size; i++)
                    {
                        string d_x = ip_dest_set[i];

                        // For packets in egress:
                        /* Nothing: We are the not main identity. */
                        // For packets in forward, received from a known MAC:
                        foreach (NeighborData neighbor in neighbors)
                        {
                            if (best_routes.has_key(neighbor.mac))
                            {
                                network_stack.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                            }
                        }
                        // For packets in forward, received from a unknown MAC:
                        if (best_routes.has_key("main"))
                            network_stack.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    null,
                                    null);
                        else network_stack.change_best_path(d_x, null, null, null, null);
                    }
                }
                else
                {
                    // Compute IP dest addresses (in this case no src addresses).
                    ArrayList<string> ip_dest_set = new ArrayList<string>();
                    // Global.
                    ip_dest_set.add(ip_global_gnode(h_addr, h.lvl));
                    // Anonymizing.
                    ip_dest_set.add(ip_anonymizing_gnode(h_addr, h.lvl));
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        ip_dest_set.add(ip_internal_gnode(h_addr, h.lvl, t));
                    }

                    for (int i = 0; i < ip_dest_set.size; i++)
                    {
                        string d_x = ip_dest_set[i];

                        // For packets in egress:
                        /* Nothing: We are the not main identity. */
                        // For packets in forward, received from a known MAC:
                        foreach (NeighborData neighbor in neighbors)
                        {
                            if (best_routes.has_key(neighbor.mac))
                            {
                                network_stack.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                network_stack.change_best_path(d_x, null, null, null, neighbor.mac);
                            }
                        }
                        // For packets in forward, received from a unknown MAC:
                        if (best_routes.has_key("main"))
                            network_stack.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    null,
                                    null);
                        else network_stack.change_best_path(d_x, null, null, null, null);
                    }
                }
            }
        }
        private HashMap<string, BestRoute> find_best_routes(
         Gee.List<IQspnNodePath> paths,
         ArrayList<NeighborData> neighbors)
        {
            HashMap<string, BestRoute> best_routes = new HashMap<string, BestRoute>();
            foreach (IQspnNodePath path in paths)
            {
                QspnArc path_arc = (QspnArc)path.i_qspn_get_arc();
                string path_dev = path_arc.arc.neighborhood_arc.nic.dev;
                string gw = path_arc.arc.neighborhood_arc.neighbour_nic_addr;
                if (best_routes.is_empty)
                {
                    string k = "main";
                    // absolute best.
                    BestRoute r = new BestRoute();
                    r.gw = gw;
                    r.dev = path_dev;
                    best_routes[k] = r;
                }
                bool completed = true;
                foreach (NeighborData neighbor in neighbors)
                {
                    // is it best without neighbor?
                    string k = neighbor.mac;
                    // best_routes contains k?
                    if (best_routes.has_key(k)) continue;
                    // path contains neighbor's g-node?
                    ArrayList<HCoord> searchable_path = new ArrayList<HCoord>((a, b) => a.equals(b));
                    foreach (IQspnHop path_h in path.i_qspn_get_hops())
                        searchable_path.add(path_h.i_qspn_get_hcoord());
                    if (neighbor.h in searchable_path)
                    {
                        completed = false;
                        continue;
                    }
                    // best without neighbor.
                    BestRoute r = new BestRoute();
                    r.gw = gw;
                    r.dev = path_dev;
                    best_routes[k] = r;
                }
                if (completed) break;
            }
            return best_routes;
        }

        public void presence_notified()
        {
            // TODO
        }

        public void qspn_bootstrap_complete()
        {
            print(@"Debug: IdentityData #$(local_identity_index): call update_all_destinations for qspn_bootstrap_complete.\n");
            update_all_destinations();
            print(@"Debug: IdentityData #$(local_identity_index): done update_all_destinations for qspn_bootstrap_complete.\n");
        }

        public void update_all_destinations()
        {
            for (int lvl = 0; lvl < levels; lvl++) for (int pos = 0; pos < _gsizes[lvl]; pos++) if (my_naddr.pos[lvl] != pos)
                update_best_path(new HCoord(lvl, pos));
        }

        public void remove_identity()
        {
            // The qspn manager wants to remove this identity.
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
            qspn_mgr.destroy();
            // We must remove identity from identity_manager. This will have IIdmgmtNetnsManager
            //  to remove pseudodevs and the network namespace. Beforehand, the NetworkStack
            //  instance has to be notified.
            network_stack.removing_namespace();
            identity_mgr.unset_identity_module(nodeid, "qspn");
            identity_mgr.remove_identity(nodeid);
            // remove identity and its id-arcs from memory data-structures
            local_identities.unset(local_identity_index);
            ArrayList<int> todel = new ArrayList<int>();
            foreach (int i in identityarcs.keys)
            {
                IdentityArc ia = identityarcs[i];
                NodeID id = ia.id;
                if (id.equals(nodeid)) todel.add(i);
            }
            foreach (int i in todel) identityarcs.unset(i);
        }
    }

    class NeighborhoodIPRouteManager : Object, INeighborhoodIPRouteManager
    {
        public void add_address(string my_addr, string my_dev)
        {
            cm.single_command(new ArrayList<string>.wrap({
                "ip", "address", "add", my_addr, "dev", my_dev}));
        }

        public void add_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            cm.single_command(new ArrayList<string>.wrap({
                "ip", "route", "add", neighbor_addr, "dev", my_dev, "src", my_addr}));
        }

        public void remove_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            cm.single_command(new ArrayList<string>.wrap({
                "ip", "route", "del", neighbor_addr, "dev", my_dev, "src", my_addr}));
        }

        public void remove_address(string my_addr, string my_dev)
        {
            cm.single_command(new ArrayList<string>.wrap({
                "ip", "address", "del", @"$(my_addr)/32", "dev", my_dev}));
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
                @"ip", @"netns", @"add", @"$ns"}));
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
                @"ip", @"netns", @"del", @"$ns"}));
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
        public NeighborhoodMissingArcHandler.from_qspn(IQspnMissingArcHandler qspn_missing, int local_identity_index)
        {
            this.qspn_missing = qspn_missing;
            this.local_identity_index = local_identity_index;
        }
        private IQspnMissingArcHandler? qspn_missing;
        private int local_identity_index;

        public void missing(INeighborhoodArc arc)
        {
            if (qspn_missing != null)
            {
                // from a INeighborhoodArc get a list of QspnArc
                foreach (IdentityArc ia in local_identities[local_identity_index].my_identityarcs) if (ia.qspn_arc != null)
                    if (ia.qspn_arc.arc.neighborhood_arc == arc)
                        qspn_missing.i_qspn_missing(ia.qspn_arc);
            }
        }
    }

    class QspnStubFactory : Object, IQspnStubFactory
    {
        public QspnStubFactory(int local_identity_index)
        {
            this.local_identity_index = local_identity_index;
        }
        private int local_identity_index;

        /* This "holder" class is needed because the QspnManagerRemote class provided by
         * the ZCD framework is owned (and tied to) by the AddressManagerXxxxRootStub.
         */
        private class QspnManagerStubHolder : Object, IQspnManagerStub
        {
            public QspnManagerStubHolder(IAddressManagerStub addr)
            {
                this.addr = addr;
            }
            private IAddressManagerStub addr;

            public IQspnEtpMessage get_full_etp(IQspnAddress requesting_address)
            throws QspnNotAcceptedError, QspnBootstrapInProgressError, StubError, DeserializeError
            {
                return addr.qspn_manager.get_full_etp(requesting_address);
            }

            public void got_destroy()
            throws StubError, DeserializeError
            {
                addr.qspn_manager.got_destroy();
            }

            public void got_prepare_destroy()
            throws StubError, DeserializeError
            {
                addr.qspn_manager.got_prepare_destroy();
            }

            public void send_etp(IQspnEtpMessage etp, bool is_full)
            throws QspnNotAcceptedError, StubError, DeserializeError
            {
                addr.qspn_manager.send_etp(etp, is_full);
            }
        }

        /* This "void" class is needed for broadcast without arcs.
         */
        private class QspnManagerStubVoid : Object, IQspnManagerStub
        {
            public IQspnEtpMessage get_full_etp(IQspnAddress requesting_address)
            throws QspnNotAcceptedError, QspnBootstrapInProgressError, StubError, DeserializeError
            {
                assert_not_reached();
            }

            public void got_destroy()
            throws StubError, DeserializeError
            {
            }

            public void got_prepare_destroy()
            throws StubError, DeserializeError
            {
            }

            public void send_etp(IQspnEtpMessage etp, bool is_full)
            throws QspnNotAcceptedError, StubError, DeserializeError
            {
            }
        }

        public IQspnManagerStub
                        i_qspn_get_broadcast(
                            Gee.List<IQspnArc> arcs,
                            IQspnMissingArcHandler? missing_handler=null
                        )
        {
            if(arcs.is_empty) return new QspnManagerStubVoid();
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
                n_missing_handler = new NeighborhoodMissingArcHandler.from_qspn(missing_handler, local_identity_index);
            }
            IAddressManagerStub addrstub = 
                neighborhood_mgr.get_stub_identity_aware_broadcast(
                source_node_id,
                broadcast_node_id_set,
                n_missing_handler);
            QspnManagerStubHolder ret = new QspnManagerStubHolder(addrstub);
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
            QspnManagerStubHolder ret = new QspnManagerStubHolder(addrstub);
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
        public QspnArc(Arc arc, NodeID sourceid, NodeID destid, string peer_mac)
        {
            this.arc = arc;
            this.sourceid = sourceid;
            this.destid = destid;
            this.peer_mac = peer_mac;
        }
        public weak Arc arc;
        public NodeID sourceid;
        public NodeID destid;
        public string peer_mac;

        public IQspnCost i_qspn_get_cost()
        {
            return new Cost(arc.cost);
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

    IAddressManagerSkeleton?
    get_identity_skeleton(
        NodeID source_id,
        NodeID unicast_id,
        string peer_address)
    {
        foreach (int local_identity_index in local_identities.keys)
        {
            NodeID local_nodeid = local_identities[local_identity_index].nodeid;
            if (local_nodeid.equals(unicast_id))
            {
                foreach (int identityarc_index in identityarcs.keys)
                {
                    IdentityArc ia = identityarcs[identityarc_index];
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address)
                    {
                        if (ia.id.equals(local_nodeid))
                        {
                            if (ia.id_arc.get_peer_nodeid().equals(source_id))
                            {
                                return local_identities[local_identity_index].addr_man;
                            }
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
            NodeID local_nodeid = local_identities[local_identity_index].nodeid;
            if (local_nodeid in broadcast_set)
            {
                foreach (int identityarc_index in identityarcs.keys)
                {
                    IdentityArc ia = identityarcs[identityarc_index];
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address
                        && _arc.neighborhood_arc.nic.dev == dev)
                    {
                        if (ia.id.equals(local_nodeid))
                        {
                            if (ia.id_arc.get_peer_nodeid().equals(source_id))
                            {
                                ret.add(local_identities[local_identity_index].addr_man);
                            }
                        }
                    }
                }
            }
        }
        return ret;
    }

    void identity_arc_added(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
        print("An identity-arc has been added.\n");
        int local_identity_index;
        IdentityData identity_data = find_or_create_local_identity(id, out local_identity_index);
        string ns = identity_data.network_namespace;
        IdentityArc ia = new IdentityArc(arc, id, id_arc, id_arc.get_peer_mac(), id_arc.get_peer_linklocal());
        int identityarc_index = identityarc_nextindex++;
        identityarcs[identityarc_index] = ia;
        identity_data.my_identityarcs.add(ia);
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).\n");
        print(@"                  my id handles $(pseudodev) on '$(ns)'.\n");
        print(@"                  on the other side this identityarc links to $(ia.peer_linklocal) == $(ia.peer_mac).\n");
    }

    void identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
        // Retrieve my identity.
        int local_identity_index;
        IdentityData identity_data = find_or_create_local_identity(id, out local_identity_index);
        print("An identity-arc has been changed.\n");
        int identityarc_index = -1;
        foreach (int i in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[i];
            if (ia.arc == arc)
            {
                if (ia.id.equals(id))
                {
                    if (ia.id_arc.get_peer_nodeid().equals(id_arc.get_peer_nodeid()))
                    {
                        identityarc_index = i;
                        break;
                    }
                }
            }
        }
        if (identityarc_index == -1)
        {
            print("I couldn't find it in memory.\n");
            return;
        }
        IdentityArc ia = identityarcs[identityarc_index];
        string old_mac = ia.peer_mac;
        string old_linklocal = ia.peer_linklocal;
        ia.peer_mac = id_arc.get_peer_mac();
        ia.peer_linklocal = id_arc.get_peer_linklocal();
        string ns = identity_data.network_namespace;
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).\n");
        print(@"                  my id handles $(pseudodev) on '$(ns)'.\n");
        print(@"                  on the other side this identityarc links to $(ia.peer_linklocal) == $(ia.peer_mac).\n");
        print(@"                  before the change, the link was to $(old_linklocal) == $(old_mac).\n");
        // This should be the same instance.
        assert(ia.id_arc == id_arc);
        // Retrieve qspn_arc if there was one for this identity-arc.
        if (ia.qspn_arc != null)
        {
            // TODO This has to be done only if this identity is not doing add_identity.
            // Update this qspn_arc
            ia.qspn_arc.peer_mac = ia.peer_mac;
            // Create a new table for neighbour, with an `unreachable` for all known destinations.
            identity_data.network_stack.add_neighbour(ia.qspn_arc.peer_mac);
            // Remove the table `ntk_from_old_mac`. It may reappear afterwards, that would be
            //  a definitely new neighbour node.
            identity_data.network_stack.remove_neighbour(old_mac);
            // In new table `ntk_from_newmac` update all routes.
            // In other tables, update all routes that have the new peer_linklocal as gateway.
            // Indeed, update best route for all known destinations.
            print(@"Debug: IdentityData #$(identity_data.local_identity_index): call update_all_destinations for identity_arc_changed.\n");
            identity_data.update_all_destinations();
            print(@"Debug: IdentityData #$(identity_data.local_identity_index): done update_all_destinations for identity_arc_changed.\n");
        }
    }

    void identity_arc_removing(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        // Retrieve my identity.
        int local_identity_index;
        IdentityData identity_data = find_or_create_local_identity(id, out local_identity_index);
        // Retrieve qspn_arc if still there.
        foreach (IdentityArc ia in identity_data.my_identityarcs)
            if (ia.arc == arc)
            if (ia.qspn_arc != null)
            if (ia.qspn_arc.destid.equals(peer_nodeid))
        {
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");
            qspn_mgr.arc_remove(ia.qspn_arc);
            // TODO string peer_mac = ia.qspn_arc.peer_mac;
            ia.qspn_arc = null;
            // TODO remove rule ntk_from_$peer_mac
        }
    }

    void identity_arc_removed(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        print("An identity-arc has been removed.\n");
        // Retrieve my identity.
        int local_identity_index;
        IdentityData identity_data = find_or_create_local_identity(id, out local_identity_index);
        // Retrieve my identity_arc.
        int identityarc_index = -1;
        foreach (int i in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[i];
            if (ia.arc == arc)
            {
                if (ia.id.equals(id))
                {
                    if (ia.id_arc.get_peer_nodeid().equals(peer_nodeid))
                    {
                        identityarc_index = i;
                        break;
                    }
                }
            }
        }
        if (identityarc_index == -1)
        {
            print("I couldn't find it in memory.\n");
            return;
        }
        IdentityArc ia = identityarcs[identityarc_index];
        string ns = identity_data.network_namespace;
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(ia.id_arc.get_peer_nodeid().id).\n");
        print(@"                  my id handles $(pseudodev) on '$(ns)'.\n");
        print(@"                  on the other side this identityarc links to $(ia.peer_linklocal) == $(ia.peer_mac).\n");
        identityarcs.unset(identityarc_index);
        identity_data.my_identityarcs.remove(ia);
    }

    void identity_mgr_arc_removed(IIdmgmtArc arc)
    {
        // Find the arc data.
        foreach (string k in real_arcs.keys)
        {
            Arc node_arc = real_arcs[k];
            if (node_arc.idmgmt_arc == arc)
            {
                // This arc has been removed from identity_mgr. Save this info.
                identity_mgr_arcs.remove(k);
                // Remove arc from neighborhood, because it fails.
                neighborhood_mgr.remove_my_arc(node_arc.neighborhood_arc, false);
                break;
            }
        }
    }

    void nic_address_set(string my_dev, string my_addr)
    {
        if (identity_mgr != null)
        {
            print(@"Warning: Signal `nic_address_set($(my_dev),$(my_addr))` when module Identities is already initialized.\n");
            print(@"         This should not happen and will be ignored.\n");
            return;
        }
        string my_mac = macgetter.get_mac(my_dev).up();
        HandledNic n = new HandledNic();
        n.dev = my_dev;
        n.mac = my_mac;
        n.linklocal = my_addr;
        handlednics.add(n);
        int i = handlednics.size - 1;
        print(@"handlednics: #$(i): $(n.dev) = $(n.mac) (has $(n.linklocal)).\n");
    }

    string key_for_physical_arc(string mymac, string peermac)
    {
        return @"$(mymac)-$(peermac)";
    }

    void arc_added(INeighborhoodArc arc)
    {
        string k = key_for_physical_arc(arc.nic.mac, arc.neighbour_mac);
        assert(! (k in neighborhood_arcs.keys));
        neighborhood_arcs[k] = arc;
        print(@"neighborhood_arc '$(k)': peer_linklocal $(arc.neighbour_nic_addr), cost $(arc.cost)us\n");
    }

    void arc_changed(INeighborhoodArc arc)
    {
        //print(@"arc_changed (no effect) for $(arc.neighbour_nic_addr)\n");
    }

    void arc_removing(INeighborhoodArc arc, bool is_still_usable)
    {
        string k = key_for_physical_arc(arc.nic.mac, arc.neighbour_mac);
        // Had this arc been added to 'real_arcs'?
        if ( ! (k in real_arcs.keys)) return;
        // Has real_arc already been removed from Identities?
        if (k in identity_mgr_arcs)
        {
            Arc real_arc = real_arcs[k];
            identity_mgr.remove_arc(real_arc.idmgmt_arc);
            identity_mgr_arcs.remove(k);
        }
        // Remove arc from real_arcs.
        real_arcs.unset(k);
    }

    void arc_removed(INeighborhoodArc arc)
    {
        string k = key_for_physical_arc(arc.nic.mac, arc.neighbour_mac);
        print(@"Neighborhood module: neighborhood_arc `$(k)` has been removed.\n");
        neighborhood_arcs.unset(k);
    }

    void nic_address_unset(string my_dev, string my_addr)
    {
    }

    string naddr_repr(Naddr my_naddr)
    {
        string my_naddr_str = "";
        string sep = "";
        for (int i = 0; i < levels; i++)
        {
            my_naddr_str = @"$(my_naddr.i_qspn_get_pos(i))$(sep)$(my_naddr_str)";
            sep = ":";
        }
        return my_naddr_str;
    }

    string fp_elderships_repr(Fingerprint my_fp)
    {
        string my_elderships_str = "";
        string sep = "";
        assert(my_fp.level == 0);
        for (int i = 0; i < levels; i++)
        {
            my_elderships_str = @"$(my_fp.elderships[i])$(sep)$(my_elderships_str)";
            sep = ":";
        }
        return my_elderships_str;
    }

    Gee.List<string> show_handlednics()
    {
        ArrayList<string> ret = new ArrayList<string>();
        int i = 0;
        foreach (HandledNic n in handlednics)
        {
            ret.add(@"handlednics: #$(i): $(n.dev) = $(n.mac) (has $(n.linklocal)).");
            i++;
        }
        return ret;
    }

    Gee.List<string> show_local_identities()
    {
        ArrayList<string> ret = new ArrayList<string>();
        foreach (int i in local_identities.keys)
        {
            ret.add_all(print_local_identity(i));
        }
        return ret;
    }

    Gee.List<string> print_local_identity(int index)
    {
        ArrayList<string> ret = new ArrayList<string>();
        IdentityData identity = local_identities[index];
        string my_naddr_str = naddr_repr(identity.my_naddr);
        string my_elderships_str = fp_elderships_repr(identity.my_fp);
        string my_fp0 = @"$(identity.my_fp.id)";
        string l0 = @"local_identity #$(index):";
        l0 += @" address $(my_naddr_str), elderships $(my_elderships_str),";
        string network_namespace_str = identity.network_namespace;
        if (network_namespace_str == "") network_namespace_str = "default";
        l0 += @" namespace $(network_namespace_str),";
        string l1 = @"                   fp0 $(my_fp0).";
        ret.add(l0);
        ret.add(l1);
        return ret;
    }

    Gee.List<string> show_neighborhood_arcs()
    {
        ArrayList<string> ret = new ArrayList<string>();
        foreach (string k in neighborhood_arcs.keys)
        {
            INeighborhoodArc arc = neighborhood_arcs[k];
            ret.add(@"neighborhood_arc '$(k)': peer_linklocal $(arc.neighbour_nic_addr), cost $(arc.cost)us");
        }
        return ret;
    }

    Gee.List<string> add_real_arc(string k, int cost)
    {
        INeighborhoodArc _arc = neighborhood_arcs[k];
        ArrayList<string> ret = new ArrayList<string>();
        // Had this arc been already added to 'real_arcs'?
        if (k in real_arcs.keys)
        {
            ret.add("Already there.");
            return ret;
        }
        Arc arc = new Arc();
        arc.cost = cost;
        arc.neighborhood_arc = _arc;
        arc.idmgmt_arc = new IdmgmtArc(arc);
        real_arcs[k] = arc;
        print(@"real_arc '$(k)': peer_linklocal $(_arc.neighbour_nic_addr), cost $(arc.cost)us\n");
        identity_mgr.add_arc(arc.idmgmt_arc);
        identity_mgr_arcs.add(k);
        return ret;
    }

    Gee.List<string> show_real_arcs()
    {
        ArrayList<string> ret = new ArrayList<string>();
        foreach (string k in real_arcs.keys)
        {
            Arc arc = real_arcs[k];
            ret.add(@"real_arc '$(k)': peer_linklocal $(arc.neighborhood_arc.neighbour_nic_addr), cost $(arc.cost)us\n");
        }
        return ret;
    }

    void change_real_arc(string k, int cost)
    {
        assert(k in real_arcs.keys);
        Arc arc = real_arcs[k];
        arc.cost = cost;
        print(@"real_arc '$(k)': peer_linklocal $(arc.neighborhood_arc.neighbour_nic_addr), cost $(arc.cost)us\n");
        foreach (NodeID node_id in identity_mgr.get_id_list())
        {
            foreach (IIdmgmtIdentityArc id_arc in identity_mgr.get_identity_arcs(arc.idmgmt_arc, node_id))
            {
                IdentityArc ia = find_identity_arc(id_arc);
                if (ia.qspn_arc != null)
                {
                    QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(node_id, "qspn");
                    qspn_mgr.arc_is_changed(ia.qspn_arc);
                }
            }
        }
    }

    void remove_real_arc(string k)
    {
        assert(k in real_arcs.keys);
        Arc arc = real_arcs[k];
        print(@"real_arc '$(k)' removed.\n");
        identity_mgr.remove_arc(arc.idmgmt_arc);
        identity_mgr_arcs.remove(k);
        real_arcs.unset(k);
    }

    Gee.List<string> show_identityarcs()
    {
        ArrayList<string> ret = new ArrayList<string>();
        foreach (int i in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[i];
            IIdmgmtArc arc = ia.arc;
            NodeID id = ia.id;
            // Retrieve my identity.
            int local_identity_index;
            IdentityData identity_data = find_or_create_local_identity(id, out local_identity_index);
            IIdmgmtIdentityArc id_arc = ia.id_arc;
            ret.add(@"identityarcs: #$(i): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),");
            ret.add(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).");
            string peer_ll = ia.id_arc.get_peer_linklocal();
            string ns = identity_data.network_namespace;
            string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
            ret.add(@"                  dev-ll: from $(pseudodev) on '$(ns)' to $(peer_ll).");
        }
        return ret;
    }

    void prepare_enter_net_phase_1(
        int local_identity_index,
        int guest_gnode_level,
        int host_gnode_level,
        string host_gnode_address,
        string host_gnode_data,
        int in_host_pos1,
        int in_host_pos1_eldership,
        int in_host_pos2,
        int in_host_pos2_eldership,
        int connectivity_pos,
        int connectivity_pos_eldership,
        ArrayList<string> id_arc_index_list,
        int op_id,
        int? prev_op_id)
    {
        string k = @"$(local_identity_index)+$(op_id)";
        assert(! (k in pending_prepared_enter_net_operations.keys));
        PreparedEnterNet pending = new PreparedEnterNet();
        pending.local_identity_index = local_identity_index;
        pending.guest_gnode_level = guest_gnode_level;
        pending.host_gnode_level = host_gnode_level;
        pending.host_gnode_address = host_gnode_address;
        pending.host_gnode_data = host_gnode_data;
        pending.in_host_pos1 = in_host_pos1;
        pending.in_host_pos1_eldership = in_host_pos1_eldership;
        pending.in_host_pos2 = in_host_pos2;
        pending.in_host_pos2_eldership = in_host_pos2_eldership;
        pending.connectivity_pos = connectivity_pos;
        pending.connectivity_pos_eldership = connectivity_pos_eldership;
        pending.id_arc_index_list = new ArrayList<string>();
        pending.id_arc_index_list.add_all(id_arc_index_list);
        pending.op_id = op_id;
        pending.prev_op_id = prev_op_id;
        pending_prepared_enter_net_operations[k] = pending;

        IdentityData id = local_identities[local_identity_index];
        NodeID old_id = id.nodeid;
        identity_mgr.prepare_add_identity(op_id, old_id);
    }

    class PreparedEnterNet : Object
    {
        public int local_identity_index;
        public int guest_gnode_level;
        public int host_gnode_level;
        public string host_gnode_address;
        public string host_gnode_data;
        public int in_host_pos1;
        public int in_host_pos1_eldership;
        public int in_host_pos2;
        public int in_host_pos2_eldership;
        public int connectivity_pos;
        public int connectivity_pos_eldership;
        public ArrayList<string> id_arc_index_list;
        public int op_id;
        public int? prev_op_id;
    }
    HashMap<string,PreparedEnterNet> pending_prepared_enter_net_operations;

    void enter_net_phase_1(
        int old_local_identity_index,
        int op_id)
    {
        string kk = @"$(old_local_identity_index)+$(op_id)";
        assert(kk in pending_prepared_enter_net_operations.keys);
        PreparedEnterNet op = pending_prepared_enter_net_operations[kk];

        IdentityData old_identity_data = local_identities[old_local_identity_index];
        NodeID old_id = old_identity_data.nodeid;
        NodeID new_id = identity_mgr.add_identity(op_id, old_id);
        // This produced some signal `identity_arc_added`: hence some IdentityArc instances have been created
        //  and stored in `new_identity_data.my_identityarcs`.
        int local_identity_index;
        IdentityData new_identity_data = find_or_create_local_identity(new_id, out local_identity_index);
        new_identity_data.copy_of_identity = old_identity_data;
        new_identity_data.connectivity_from_level = old_identity_data.connectivity_from_level;
        new_identity_data.connectivity_to_level = old_identity_data.connectivity_to_level;
        old_identity_data.connectivity_from_level = /*TODO*/0;
        old_identity_data.connectivity_to_level = /*TODO*/0;

        HashMap<IdentityArc, IdentityArc> old_to_new_id_arc = new HashMap<IdentityArc, IdentityArc>();
        foreach (IdentityArc w0 in old_identity_data.my_identityarcs)
        {
            bool check_peer_mac = true;
            if (w0.peer_mac == w0.id_arc.get_peer_mac()) check_peer_mac = false;
            foreach (IdentityArc w1 in new_identity_data.my_identityarcs)
            {
                if (w1.arc != w0.arc) continue;
                if (check_peer_mac)
                {
                    if (w1.peer_mac != w0.id_arc.get_peer_mac()) continue;
                }
                else
                {
                    if (! w1.id_arc.get_peer_nodeid().equals(w0.id_arc.get_peer_nodeid())) continue;
                }
                old_to_new_id_arc[w0] = w1;
                break;
            }
        }

        // TODO Spostamento delle rotte della vecchia identità
        string old_ns = new_identity_data.network_namespace;
        string new_ns = old_identity_data.network_namespace;
        ArrayList<int> _naddr_old = new ArrayList<int>();
        _naddr_old.add_all(old_identity_data.my_naddr.pos);
        Naddr my_naddr_old = new Naddr(_naddr_old.to_array(), _gsizes.to_array());
        ArrayList<int> _naddr_conn = new ArrayList<int>();
        _naddr_conn.add_all(old_identity_data.my_naddr.pos);
        _naddr_conn[op.guest_gnode_level] = op.connectivity_pos;
        Naddr my_naddr_conn = new Naddr(_naddr_conn.to_array(), _gsizes.to_array());
        HashMap<int,HashMap<int,DestinationIPSet>> old_destination_ip_set;
        old_destination_ip_set = copy_destination_ip_set(old_identity_data.destination_ip_set);
        compute_destination_ip_set(old_identity_data.destination_ip_set, my_naddr_conn);

        ArrayList<string> prefix_cmd_old_ns = new ArrayList<string>();
        if (old_ns != "") prefix_cmd_old_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$old_ns"});
        ArrayList<string> prefix_cmd_new_ns = new ArrayList<string>.wrap({
            @"ip", @"netns", @"exec", @"$new_ns"});

        // Search qspn-arc of old identity
        foreach (IdentityArc ia in old_identity_data.my_identityarcs) if (ia.qspn_arc != null)
        {
            // TODO
        }

        // Remove old destination IPs from all tables in old network namespace
        int bid = cm.begin_block();
        ArrayList<string> tablenames = new ArrayList<string>();
        if (old_ns == "") tablenames.add("ntk");
        // Add a table for each qspn-arc of old identity
        foreach (IdentityArc ia in old_identity_data.my_identityarcs) if (ia.qspn_arc != null)
        {
            int tid;
            string tablename;
            tn.get_table(ia.peer_mac, out tid, out tablename);
            // Note: Member peer_mac is not changed yet (in case of a g-node migration)
            // It is the old one. Whilst ia.id_arc.get_peer_mac() might differ.
            tablenames.add(tablename);
        }
        foreach (string tablename in tablenames)
         for (int i = levels-1; i >= subnetlevel; i--)
         for (int j = 0; j < _gsizes[i]; j++)
        {
            if (old_destination_ip_set[i][j].global != "")
            {
                string ipaddr = old_destination_ip_set[i][j].global;
                ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"del", @"$ipaddr", @"table", @"$tablename"});
                cm.single_command_in_block(bid, cmd);
                ipaddr = old_destination_ip_set[i][j].anonymous;
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"del", @"$ipaddr", @"table", @"$tablename"});
                cm.single_command_in_block(bid, cmd);
            }
            for (int k = levels-1; k >= i+1; k--)
            {
                if (old_destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = old_destination_ip_set[i][j].intern[k];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"del", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                }
            }
        }
        cm.end_block(bid);

        // Remove addresses
        if (old_ns == "")
        {
            // remove SNAT rule
            if (! no_anonymize && old_identity_data.local_ip_set.global != "")
            {
                string anonymousrange = ip_anonymizing_gnode(old_identity_data.my_naddr.pos, levels);
                cm.single_command(new ArrayList<string>.wrap({
                    @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$anonymousrange",
                    @"-j", @"SNAT", @"--to", @"$(old_identity_data.local_ip_set.global)"}));
            }

            // remove local addresses (global, anon, intern)
            if (old_identity_data.local_ip_set.global != "")
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.global)/32", @"dev", @"$dev"}));
            if (old_identity_data.local_ip_set.anonymous != "" && accept_anonymous_requests)
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.anonymous)/32", @"dev", @"$dev"}));
            for (int i = levels-1; i > op.guest_gnode_level; i--)
            {
                if (old_identity_data.local_ip_set.intern[i] != "")
                    foreach (string dev in real_nics)
                    cm.single_command(new ArrayList<string>.wrap({
                        @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.intern[i])/32", @"dev", @"$dev"}));
            }
        }

        // New qspn manager
        Naddr new_id_naddr = null; // TODO
        ArrayList<IQspnArc> internal_arc_set = null; // TODO
        ArrayList<IQspnNaddr> internal_arc_peer_naddr_set = null; // TODO
        ArrayList<IQspnArc> external_arc_set = null; // TODO
        QspnManager.PreviousArcToNewArcDelegate old_arc_to_new_arc = (/*IQspnArc*/ old_arc) => {
            IQspnArc? ret = null;
            // TODO get new arc
            return ret;
        };
        Fingerprint new_id_fp = null; // TODO
        QspnManager qspn_mgr = new QspnManager.enter_net(new_id_naddr,
            internal_arc_set,
            internal_arc_peer_naddr_set,
            external_arc_set,
            old_arc_to_new_arc,
            new_id_fp,
            new QspnStubFactory(local_identity_index),
            /*hooking_gnode_level*/ op.guest_gnode_level,
            /*into_gnode_level*/ op.host_gnode_level,
            /*previous_identity*/ (QspnManager)(identity_mgr.get_identity_module(old_id, "qspn")));

        identity_mgr.set_identity_module(new_id, "qspn", qspn_mgr);
        new_identity_data.my_naddr = new_id_naddr;
        new_identity_data.my_fp = new_id_fp;
        new_identity_data.ready = false;
        new_identity_data.addr_man = new AddressManagerForIdentity(qspn_mgr);

        error("not implemented yet");
    }

    void add_qspnarc(int local_identity_index, int idarc_index)
    {
        IdentityData identity = local_identities[local_identity_index];
        NodeID id = identity.nodeid;
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");

        assert(idarc_index in identityarcs.keys);
        IdentityArc ia = identityarcs[idarc_index];
        NodeID destid = ia.id_arc.get_peer_nodeid();
        NodeID sourceid = ia.id;
        IdmgmtArc __arc = (IdmgmtArc)ia.arc;
        Arc _arc = __arc.arc;
        string peer_mac = ia.id_arc.get_peer_mac();
        ia.qspn_arc = new QspnArc(_arc, sourceid, destid, peer_mac);
        qspn_mgr.arc_add(ia.qspn_arc);
        if (! (peer_mac in identity.network_stack.current_neighbours))
            identity.network_stack.add_neighbour(peer_mac);
        print(@"Debug: IdentityData #$(identity.local_identity_index): call update_all_destinations for add_qspnarc.\n");
        identity.update_all_destinations();
        print(@"Debug: IdentityData #$(identity.local_identity_index): done update_all_destinations for add_qspnarc.\n");
    }

    Gee.List<string> check_connectivity(int local_identity_index)
    {
        ArrayList<string> ret = new ArrayList<string>();
        NodeID id = local_identities[local_identity_index].nodeid;
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");
        bool _ret = qspn_mgr.check_connectivity();
        if (_ret) ret.add("This identity can be removed.");
        else ret.add("This identity CANNOT be removed.");
        return ret;
    }
}

