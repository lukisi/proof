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

    ITasklet tasklet;
    ArrayList<int> _gsizes;
    ArrayList<int> _g_exp;
    int levels;
    NeighborhoodManager? neighborhood_mgr;
    IdentityManager? identity_mgr;
    ArrayList<string> real_nics;
    int linklocal_nextindex;
    HashMap<int, HandledNic> linklocals;
    HashMap<string, HandledNic> current_nics;
    int nodeid_nextindex;
    HashMap<int, IdentityData> nodeids;
    HashMap<string, INeighborhoodArc> neighborhood_arcs;
    int nodearc_nextindex;
    HashMap<int, Arc> nodearcs;
    int identityarc_nextindex;
    HashMap<int, IdentityArc> identityarcs;

    AddressManagerForNode node_skeleton;
    ServerDelegate dlg;
    ServerErrorHandler err;
    ArrayList<ITaskletHandle> t_udp_list;

    int main(string[] args)
    {
        accept_anonymous_requests = false; // default
        no_anonymize = false; // default
        OptionContext oc = new OptionContext("<topology> <address>");
        OptionEntry[] entries = new OptionEntry[4];
        int index = 0;
        entries[index++] = {"interfaces", 'i', 0, OptionArg.STRING_ARRAY, ref interfaces, "Interface (e.g. -i eth1). You can use it multiple times.", null};
        entries[index++] = {"serve-anonymous", 'k', 0, OptionArg.NONE, ref accept_anonymous_requests, "Accept anonymous requests", null};
        entries[index++] = {"no-anonymize", 'j', 0, OptionArg.NONE, ref no_anonymize, "Disable anonymizer", null};
        entries[index++] = { null };
        oc.add_main_entries(entries, null);
        try {
            oc.parse(ref args);
        }
        catch (OptionError e) {
            print(@"Error parsing options: $(e.message)\n");
            return 1;
        }

        if (args.length < 3) error("You have to set your topology (args[1]) and address (args[2]).");
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

        prepare_all_nics();
        // Pass tasklet system to the RPC library (ntkdrpc)
        init_tasklet_system(tasklet);

        dlg = new ServerDelegate();
        err = new ServerErrorHandler();

        // Handle for TCP
        ITaskletHandle t_tcp;
        // Handles for UDP
        t_udp_list = new ArrayList<ITaskletHandle>();

        // start listen TCP
        t_tcp = tcp_listen(dlg, err, ntkd_port);

        real_nics = new ArrayList<string>();
        linklocal_nextindex = 0;
        linklocals = new HashMap<int, HandledNic>();
        current_nics = new HashMap<string, HandledNic>();
        nodeid_nextindex = 0;
        nodeids = new HashMap<int, IdentityData>();
        neighborhood_arcs = new HashMap<string, INeighborhoodArc>();
        nodearc_nextindex = 0;
        nodearcs = new HashMap<int, Arc>();
        identityarc_nextindex = 0;
        identityarcs = new HashMap<int, IdentityArc>();
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
        neighborhood_mgr.arc_removed.connect(arc_removed);
        foreach (string dev in _devs) manage_real_nic(dev);
        Gee.List<string> if_list_dev = new ArrayList<string>();
        Gee.List<string> if_list_mac = new ArrayList<string>();
        Gee.List<string> if_list_linklocal = new ArrayList<string>();
        foreach (HandledNic n in linklocals.values)
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
        node_skeleton.identity_mgr = identity_mgr;
        identity_mgr.identity_arc_added.connect(identity_arc_added);
        identity_mgr.identity_arc_changed.connect(identity_arc_changed);
        identity_mgr.identity_arc_removed.connect(identity_arc_removed);

        // First identity
        NodeID nodeid = identity_mgr.get_main_id();
        int nodeid_index = nodeid_nextindex++;
        nodeids[nodeid_index] = new IdentityData(nodeid);
        nodeids[nodeid_index].nodeid_index = nodeid_index;
        nodeids[nodeid_index].main_id = true;
        print(@"nodeids: #$(nodeid_index): $(nodeid.id).\n");
        // First qspn manager
        QspnManager.init(tasklet, max_paths, max_common_hops_ratio, arc_timeout, new ThresholdCalculator());
        Naddr my_naddr = new Naddr(_naddr.to_array(), _gsizes.to_array());
        Fingerprint my_fp = new Fingerprint(_elderships.to_array());
        string my_naddr_str = naddr_repr(my_naddr);
        string my_elderships_str = fp_elderships_repr(my_fp);
        print(@"First identity is $(my_naddr_str), elderships = $(my_elderships_str), fingerprint = $(my_fp.id).\n");
        QspnManager qspn_mgr = new QspnManager.create_net(my_naddr,
            my_fp,
            new QspnStubFactory(nodeid_index));
        identity_mgr.set_identity_module(nodeid, "qspn", qspn_mgr);
        nodeids[nodeid_index].my_naddr = my_naddr;
        nodeids[nodeid_index].my_fp = my_fp;
        nodeids[nodeid_index].ready = true;
        nodeids[nodeid_index].addr_man = new AddressManagerForIdentity(qspn_mgr);

        string ns = identity_mgr.get_namespace(nodeid);
        ArrayList<string> pseudodevs = new ArrayList<string>();
        foreach (string real_nic in real_nics) pseudodevs.add(identity_mgr.get_pseudodev(nodeid, real_nic));
        LinuxRoute route = new LinuxRoute(ns, ip_whole_network());
        nodeids[nodeid_index].route = route;
        nodeids[nodeid_index].ip_global = ip_global_node(my_naddr.pos);
        foreach (string dev in pseudodevs) route.add_address(nodeids[nodeid_index].ip_global, dev);
        if (accept_anonymous_requests)
        {
            nodeids[nodeid_index].ip_anonymizing = ip_anonymizing_node(my_naddr.pos);
            foreach (string dev in pseudodevs) route.add_address(nodeids[nodeid_index].ip_anonymizing, dev);
        }
        nodeids[nodeid_index].ip_internal = new ArrayList<string>();
        for (int j = 0; j <= levels-2; j++)
        {
            nodeids[nodeid_index].ip_internal.add(ip_internal_node(my_naddr.pos, j+1));
            foreach (string dev in pseudodevs) route.add_address(nodeids[nodeid_index].ip_internal[j], dev);
        }

        qspn_mgr.arc_removed.connect(nodeids[nodeid_index].arc_removed);
        qspn_mgr.changed_fp.connect(nodeids[nodeid_index].changed_fp);
        qspn_mgr.changed_nodes_inside.connect(nodeids[nodeid_index].changed_nodes_inside);
        qspn_mgr.destination_added.connect(nodeids[nodeid_index].destination_added);
        qspn_mgr.destination_removed.connect(nodeids[nodeid_index].destination_removed);
        qspn_mgr.gnode_splitted.connect(nodeids[nodeid_index].gnode_splitted);
        qspn_mgr.path_added.connect(nodeids[nodeid_index].path_added);
        qspn_mgr.path_changed.connect(nodeids[nodeid_index].path_changed);
        qspn_mgr.path_removed.connect(nodeids[nodeid_index].path_removed);
        qspn_mgr.presence_notified.connect(nodeids[nodeid_index].presence_notified);
        qspn_mgr.qspn_bootstrap_complete.connect(nodeids[nodeid_index].qspn_bootstrap_complete);
        qspn_mgr.remove_identity.connect(nodeids[nodeid_index].remove_identity);

        // end startup

        // start a tasklet to get commands from stdin.
        CommandLineInterfaceTasklet ts = new CommandLineInterfaceTasklet();
        tasklet.spawn(ts);

        // register handlers for SIGINT and SIGTERM to exit
        Posix.signal(Posix.SIGINT, safe_exit);
        Posix.signal(Posix.SIGTERM, safe_exit);
        // Main loop
        while (true)
        {
            tasklet.ms_wait(100);
            if (do_me_exit) break;
        }

        // TODO cleanup

        // Remove identities and their network namespaces and linklocal addresses.
        foreach (int i in nodeids.keys)
        {
            IdentityData identity_data = nodeids[i];
            if (! identity_data.main_id)
            {
                identity_data.route.removing_namespace();
                identity_mgr.remove_identity(identity_data.nodeid);
            }
        }

        // Cleanup addresses and routes that were added previously in order to
        //  obey to the qspn_mgr which is now in default network namespace.
        foreach (int i in nodeids.keys)
        {
            IdentityData identity_data = nodeids[i];
            if (identity_data.main_id)
            {
                LinuxRoute main_linux_route = identity_data.route;
                main_linux_route.stop_management();
                // Do I have a *real* Netsukuku address?
                int real_up_to = identity_data.my_naddr.get_real_up_to();
                if (real_up_to == levels-1)
                {
                    identity_data.ip_global = ip_global_node(identity_data.my_naddr.pos);
                    foreach (string dev in real_nics)
                        main_linux_route.remove_address(identity_data.ip_global, dev);
                    if (accept_anonymous_requests)
                    {
                        identity_data.ip_anonymizing = ip_anonymizing_node(identity_data.my_naddr.pos);
                        foreach (string dev in real_nics)
                            main_linux_route.remove_address(identity_data.ip_anonymizing, dev);
                    }
                }
                identity_data.ip_internal = new ArrayList<string>();
                for (int j = 0; j <= levels-2 && j <= real_up_to; j++)
                {
                    identity_data.ip_internal.add(ip_internal_node(identity_data.my_naddr.pos, j+1));
                    foreach (string dev in real_nics)
                        main_linux_route.remove_address(identity_data.ip_internal[j], dev);
                }
            }
        }
        nodeids.clear();

        // This will destroy the object NeighborhoodManager and hence call
        //  its stop_monitor_all.
        // Beware that node_skeleton.neighborhood_mgr
        //  is a weak reference.
        // Beware also that since we destroy the object, we won't receive
        //  any more signal from it, such as nic_address_unset for all the
        //  linklocal addresses that will be removed from the NICs or
        //  arc_removed.
        //  So, before doing this we need to remove all arcs.
        foreach (INeighborhoodArc arc in neighborhood_mgr.current_arcs())
            neighborhood_mgr.remove_my_arc(arc);
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
        prepare_nic(dev);
        // Start listen UDP on dev
        t_udp_list.add(udp_listen(dlg, err, ntkd_port, dev));
        // Run monitor
        neighborhood_mgr.start_monitor(new NeighborhoodNetworkInterface(dev));
        // Here the linklocal address has been added, and the signal handler for
        //  nic_address_set has been processed, so the module Identities gets its knowledge.
    }

    void prepare_all_nics(string ns_prefix="")
    {
        // disable rp_filter
        set_sys_ctl("net.ipv4.conf.all.rp_filter", "0", ns_prefix);
        // arp policies
        set_sys_ctl("net.ipv4.conf.all.arp_ignore", "1", ns_prefix);
        set_sys_ctl("net.ipv4.conf.all.arp_announce", "2", ns_prefix);
    }

    void prepare_nic(string nic, string ns_prefix="")
    {
        // disable rp_filter
        set_sys_ctl(@"net.ipv4.conf.$(nic).rp_filter", "0", ns_prefix);
        // arp policies
        set_sys_ctl(@"net.ipv4.conf.$(nic).arp_ignore", "1", ns_prefix);
        set_sys_ctl(@"net.ipv4.conf.$(nic).arp_announce", "2", ns_prefix);
    }

    void set_sys_ctl(string key, string val, string ns_prefix="")
    {
        try {
            TaskletCommandResult com_ret = tasklet.exec_command(@"$(ns_prefix)sysctl $(key)=$(val)");
            if (com_ret.exit_status != 0)
                error(@"$(com_ret.stderr)");
            com_ret = tasklet.exec_command(@"$(ns_prefix)sysctl -n $(key)");
            if (com_ret.exit_status != 0)
                error(@"$(com_ret.stderr)");
            if (com_ret.stdout != @"$(val)\n")
                error(@"Failed to set key '$(key)' to val '$(val)': now it reports '$(com_ret.stdout)'\n");
        } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
    }

    class CommandLineInterfaceTasklet : Object, ITaskletSpawnable
    {
        public void * func()
        {
            try {
                while (true)
                {
                    print("Ok> ");
                    uint8 buf[256];
                    size_t len = tasklet.read(0, (void*)buf, buf.length);
                    if (len > 255) error("Error during read of CLI: line too long");
                    string line = (string)buf;
                    if (line.has_suffix("\n")) line = line.substring(0, line.length-1);
                    ArrayList<string> _args = new ArrayList<string>();
                    foreach (string s_piece in line.split(" ")) _args.add(s_piece);
                    if (_args.size == 0)
                    {}
                    else if (_args[0] == "quit")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        do_me_exit = true;
                    }
                    else if (_args[0] == "show_linklocals")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        show_linklocals();
                    }
                    else if (_args[0] == "show_nodeids")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        show_nodeids();
                    }
                    else if (_args[0] == "show_neighborhood_arcs")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        show_neighborhood_arcs();
                    }
                    else if (_args[0] == "add_node_arc")
                    {
                        if (_args.size != 3)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        string k = _args[1];
                        int i_cost = int.parse(_args[2]);
                        if (! (k in neighborhood_arcs.keys))
                        {
                            print(@"wrong key '$(k)'\n");
                            continue;
                        }
                        add_node_arc(neighborhood_arcs[k], i_cost);
                    }
                    else if (_args[0] == "show_nodearcs")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        show_nodearcs();
                    }
                    else if (_args[0] == "change_nodearc")
                    {
                        if (_args.size != 3)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int nodearc_index = int.parse(_args[1]);
                        if (! (nodearc_index in nodearcs.keys))
                        {
                            print(@"wrong nodearc_index '$(nodearc_index)'\n");
                            continue;
                        }
                        int i_cost = int.parse(_args[2]);
                        change_nodearc(nodearc_index, i_cost);
                    }
                    else if (_args[0] == "remove_nodearc")
                    {
                        if (_args.size != 2)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int nodearc_index = int.parse(_args[1]);
                        if (! (nodearc_index in nodearcs.keys))
                        {
                            print(@"wrong nodearc_index '$(nodearc_index)'\n");
                            continue;
                        }
                        remove_nodearc(nodearc_index);
                    }
                    else if (_args[0] == "show_identityarcs")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        show_identityarcs();
                    }
                    else if (_args[0] == "show_ntkaddress")
                    {
                        if (_args.size != 2)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int nodeid_index = int.parse(_args[1]);
                        if (! (nodeid_index in nodeids.keys))
                        {
                            print(@"wrong nodeid_index '$(nodeid_index)'\n");
                            continue;
                        }
                        show_ntkaddress(nodeid_index);
                    }
                    else if (_args[0] == "prepare_add_identity")
                    {
                        if (_args.size != 3)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int migration_id = int.parse(_args[1]);
                        int nodeid_index = int.parse(_args[2]);
                        if (! (nodeid_index in nodeids.keys))
                        {
                            print(@"wrong nodeid_index '$(nodeid_index)'\n");
                            continue;
                        }
                        prepare_add_identity(migration_id, nodeid_index);
                    }
                    else if (_args[0] == "add_identity")
                    {
                        if (_args.size != 3)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int migration_id = int.parse(_args[1]);
                        int nodeid_index = int.parse(_args[2]);
                        if (! (nodeid_index in nodeids.keys))
                        {
                            print(@"wrong nodeid_index '$(nodeid_index)'\n");
                            continue;
                        }
                        add_identity(migration_id, nodeid_index);
                    }
                    else if (_args[0] == "enter_net")
                    {
                        if (_args.size < 9)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int pairs = _args.size - 9;
                        int pairs2 = pairs / 2;
                        if (pairs != pairs2 * 2)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int new_nodeid_index = int.parse(_args[1]);
                        if (! (new_nodeid_index in nodeids.keys))
                        {
                            print(@"wrong new_nodeid_index '$(new_nodeid_index)'\n");
                            continue;
                        }
                        if (nodeids[new_nodeid_index].ready)
                        {
                            print(@"wrong new_nodeid_index '$(new_nodeid_index)' (it is already started)\n");
                            continue;
                        }
                        int previous_nodeid_index = int.parse(_args[2]);
                        if (! (previous_nodeid_index in nodeids.keys))
                        {
                            print(@"wrong previous_nodeid_index '$(previous_nodeid_index)'\n");
                            continue;
                        }
                        if (! nodeids[previous_nodeid_index].ready)
                        {
                            print(@"wrong previous_nodeid_index '$(previous_nodeid_index)' (it is not started)\n");
                            continue;
                        }
                        string s_naddr_new_gnode = _args[3];
                        string s_elderships_new_gnode = _args[4];
                        int hooking_gnode_level = int.parse(_args[5]);
                        int into_gnode_level = int.parse(_args[6]);
                        int i = 7;
                        Gee.List<int> idarc_index_set = new ArrayList<int>();
                        Gee.List<string> idarc_address_set = new ArrayList<string>();
                        while (i < _args.size)
                        {
                            assert(i+1 < _args.size);
                            int idarc_index = int.parse(_args[i]);
                            idarc_index_set.add(idarc_index);
                            string idarc_address = _args[i+1];
                            idarc_address_set.add(idarc_address);
                            i += 2;
                        }
                        enter_net(new_nodeid_index,
                            previous_nodeid_index,
                            s_naddr_new_gnode,
                            s_elderships_new_gnode,
                            hooking_gnode_level,
                            into_gnode_level,
                            idarc_index_set,
                            idarc_address_set);
                    }
                    else if (_args[0] == "add_qspnarc")
                    {
                        if (_args.size != 4)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        int nodeid_index = int.parse(_args[1]);
                        int idarc_index = int.parse(_args[2]);
                        string s_naddr_neighbour = _args[3];
                        add_qspnarc(nodeid_index, idarc_index, s_naddr_neighbour);
                    }
                    else if (_args[0] == "help")
                    {
                        if (_args.size != 1)
                        {
                            print(@"Bad arguments number.\n");
                            continue;
                        }
                        print("""
Command list:

> show_linklocals
  List current link-local addresses.

> show_nodeids
  List current NodeID values.

> show_neighborhood_arcs
  List current usable arcs.

> add_node_arc <from_MAC>-<to_MAC> <cost>
  Notify a arc to IdentityManager.
  You choose a cost in microseconds for it.

> show_nodearcs
  List current accepted arcs.

> change_nodearc <nodearc_index> <cost>
  Change the cost (in microsecond) for a given arc, which was already accepted.

> remove_nodearc <nodearc_index>
  Remove a given arc, which was already accepted.

> show_identityarcs
  List current identity-arcs.

> show_ntkaddress <nodeid_index>
  Show address and elderships of one of my identities.

> prepare_add_identity <migration_id> <nodeid_index>
  Prepare to create new identity.

> add_identity <migration_id> <nodeid_index>
  Create new identity.

> enter_net <new_nodeid_index>
            <previous_nodeid_index>
            <address_new_gnode>
            <elderships_new_gnode>
            <hooking_gnode_level>
            <into_gnode_level>
                  <identityarc_index>    -| one or more times
                  <identityarc_address>  -|
  Enter network (migrate) with a newly created identity.

> add_qspnarc <nodeid_index> <identityarc_index> <identityarc_address>
  Add a QspnArc.

> help
  Show this menu.

> quit
  Exit. You can also press <ctrl-C>.

""");
                    }
                    else
                    {
                        print("CLI: unknown command\n");
                    }
                }
            } catch (Error e) {
                error(@"Error during read of CLI: $(e.message)");
            }
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

    class IdentityData : Object
    {
        public IdentityData(NodeID nodeid)
        {
            this.nodeid = nodeid;
            ready = false;
            my_arcs = new ArrayList<QspnArc>((a, b) => a.i_qspn_equals(b));
        }

        public NodeID nodeid;
        public int nodeid_index;
        public Naddr my_naddr;
        public Fingerprint my_fp;
        public bool ready;
        public AddressManagerForIdentity addr_man;
        public ArrayList<QspnArc> my_arcs;
        public LinuxRoute route;
        public bool main_id;
        public string ip_global;
        public string ip_anonymizing;
        public ArrayList<string> ip_internal;

        public void arc_removed(IQspnArc arc, bool bad_link)
        {
            // TODO
            // we should remove paths via this gateway
            error("not implemented yet");
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
            if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.
            // add a path to 'h' that says 'unreachable'.

            // Compute Netsukuku address of `h`.
            ArrayList<int> h_addr = new ArrayList<int>();
            h_addr.add_all(my_naddr.pos);
            h_addr[h.lvl] = h.pos;
            for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

            // Operations now are based on type of my_naddr:
            // Is this the main ID? Do I have a *real* Netsukuku address?
            int real_up_to = my_naddr.get_real_up_to();
            int virtual_up_to = my_naddr.get_virtual_up_to();
            if (main_id)
            {
                if (real_up_to == levels-1)
                {
                    // Global.
                    route.add_destination(ip_global_gnode(h_addr, h.lvl));
                    // Anonymizing.
                    route.add_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                }
                else
                {
                    if (h.lvl <= real_up_to)
                    {
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
                            route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else if (h.lvl < virtual_up_to)
                    {

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
                            route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else
                    {
                        // Global.
                        route.add_destination(ip_global_gnode(h_addr, h.lvl));
                        // Anonymizing.
                        route.add_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                        // Internals. In this case they are guaranteed to be valid.
                        for (int t = h.lvl + 1; t <= levels - 1; t++)
                        {
                            route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    } 
                }
            }
            else
            {
                if (h.lvl < virtual_up_to)
                {
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
                        route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                }
                else
                {
                    // Global.
                    route.add_destination(ip_global_gnode(h_addr, h.lvl));
                    // Anonymizing.
                    route.add_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        route.add_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                } 
            }
        }

        public void destination_removed(HCoord h)
        {
            if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.
            // remove any path to 'h'.

            // Compute Netsukuku address of `h`.
            ArrayList<int> h_addr = new ArrayList<int>();
            h_addr.add_all(my_naddr.pos);
            h_addr[h.lvl] = h.pos;
            for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

            // Operations now are based on type of my_naddr:
            // Is this the main ID? Do I have a *real* Netsukuku address?
            int real_up_to = my_naddr.get_real_up_to();
            int virtual_up_to = my_naddr.get_virtual_up_to();
            if (main_id)
            {
                if (real_up_to == levels-1)
                {
                    // Global.
                    route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                    // Anonymizing.
                    route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                }
                else
                {
                    if (h.lvl <= real_up_to)
                    {
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
                            route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else if (h.lvl < virtual_up_to)
                    {

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
                            route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else
                    {
                        // Global.
                        route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                        // Anonymizing.
                        route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                        // Internals. In this case they are guaranteed to be valid.
                        for (int t = h.lvl + 1; t <= levels - 1; t++)
                        {
                            route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    } 
                }
            }
            else
            {
                if (h.lvl < virtual_up_to)
                {
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
                        route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                }
                else
                {
                    // Global.
                    route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                    // Anonymizing.
                    route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                    // Internals. In this case they are guaranteed to be valid.
                    for (int t = h.lvl + 1; t <= levels - 1; t++)
                    {
                        route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                    }
                } 
            }
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
            if (! qspn_mgr.is_bootstrap_complete()) return; // not ready yet.
            if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.
            print(@"Identity #$(nodeid_index): update_best_path for h ($(h.lvl), $(h.pos)): started.\n");
            // change the route. place current best path to `h`. if none, then change the path to 'unreachable'.

            // Retrieve all routes towards `h`.
            Gee.List<IQspnNodePath> paths;
            try {
                paths = qspn_mgr.get_paths_to(h);
            } catch (QspnBootstrapInProgressError e) {
                assert_not_reached();
            }
            // If we come from a signal `path_removed`, this could be the last path
            //  towards `h` that is gone, so we might have no paths at all.
            // In this case we can just do nothing right now; in just a moment we'll
            //  also get the signal `destination_removed` which will take care.
            if (paths.is_empty) return;

            // Compute Netsukuku address of `h`.
            ArrayList<int> h_addr = new ArrayList<int>();
            h_addr.add_all(my_naddr.pos);
            h_addr[h.lvl] = h.pos;
            for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

            // Compute list of neighbors. TODO this might be done only once at start; the list should
            //  be reevaluated only when `my_arcs` changes or when `my_naddr` changes.
            ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
            foreach (QspnArc qspn_arc in my_arcs)
            {
                Arc arc = qspn_arc.arc;
                Naddr neighbour_naddr = qspn_arc.neighbour_naddr;
                INeighborhoodArc neighborhood_arc = arc.neighborhood_arc;
                NeighborData neighbor = new NeighborData();
                neighbor.mac = neighborhood_arc.neighbour_mac;
                neighbor.h = my_naddr.i_qspn_get_coord_by_address(neighbour_naddr);
                neighbors.add(neighbor);
            }

            // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
            HashMap<string, BestRoute> best_routes = find_best_routes(paths, neighbors);
            assert(best_routes.has_key("main"));

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
                        route.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    n_x,
                                    null);
                        // For packets in forward, received from a known MAC:
                        foreach (NeighborData neighbor in neighbors)
                        {
                            if (best_routes.has_key(neighbor.mac))
                            {
                                route.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                route.change_best_path(d_x, null, null, null, neighbor.mac);
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
                            route.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        n_x,
                                        null);
                            // For packets in forward, received from a known MAC:
                            foreach (NeighborData neighbor in neighbors)
                            {
                                if (best_routes.has_key(neighbor.mac))
                                {
                                    route.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    route.change_best_path(d_x, null, null, null, neighbor.mac);
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
                                    route.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    route.change_best_path(d_x, null, null, null, neighbor.mac);
                                }
                            }
                            // For packets in forward, received from a unknown MAC:
                            route.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        null,
                                        null);
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
                                    route.change_best_path(d_x,
                                        best_routes[neighbor.mac].dev,
                                        best_routes[neighbor.mac].gw,
                                        null,
                                        neighbor.mac);
                                }
                                else
                                {
                                    // set unreachable
                                    route.change_best_path(d_x, null, null, null, neighbor.mac);
                                }
                            }
                            // For packets in forward, received from a unknown MAC:
                            route.change_best_path(d_x,
                                        best_routes["main"].dev,
                                        best_routes["main"].gw,
                                        null,
                                        null);
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
                                route.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                route.change_best_path(d_x, null, null, null, neighbor.mac);
                            }
                        }
                        // For packets in forward, received from a unknown MAC:
                        route.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    null,
                                    null);
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
                                route.change_best_path(d_x,
                                    best_routes[neighbor.mac].dev,
                                    best_routes[neighbor.mac].gw,
                                    null,
                                    neighbor.mac);
                            }
                            else
                            {
                                // set unreachable
                                route.change_best_path(d_x, null, null, null, neighbor.mac);
                            }
                        }
                        // For packets in forward, received from a unknown MAC:
                        route.change_best_path(d_x,
                                    best_routes["main"].dev,
                                    best_routes["main"].gw,
                                    null,
                                    null);
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
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(nodeid, "qspn");
            try {
                foreach (HCoord h in qspn_mgr.get_known_destinations())
                {
                    update_best_path(h);
                }
            } catch (QspnBootstrapInProgressError e) {
                assert_not_reached();
            }
        }

        public void remove_identity()
        {
            // The qspn manager wants to remove this identity. We have to remove
            //  identity from identity_manager. This will have IIdmgmtNetnsManager
            //  to remove pseudodevs and the network namespace. Beforehand, the LinuxRoute
            //  instance has to be notified.
            route.removing_namespace();
            // TODO
        }
    }

    class NeighborhoodIPRouteManager : Object, INeighborhoodIPRouteManager
    {
        public void add_address(string my_addr, string my_dev)
        {
            try {
                string cmd = @"ip address add $(my_addr) dev $(my_dev)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void add_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            try {
                string cmd = @"ip route add $(neighbor_addr) dev $(my_dev) src $(my_addr)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void remove_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            try {
                string cmd = @"ip route del $(neighbor_addr) dev $(my_dev) src $(my_addr)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void remove_address(string my_addr, string my_dev)
        {
            try {
                string cmd = @"ip address del $(my_addr)/32 dev $(my_dev)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
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
            try {
                string cmd = @"ip netns add $(ns)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void create_pseudodev(string dev, string ns, string pseudo_dev, out string pseudo_mac)
        {
            assert(ns != "");
            try {
                string cmd = @"ip link add dev $(pseudo_dev) link $(dev) type macvlan";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
                pseudo_mac = macgetter.get_mac(pseudo_dev).up();
                cmd = @"ip link set dev $(pseudo_dev) netns $(ns)";
                print(@"$(cmd)\n");
                com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
                prepare_nic(pseudo_dev, @"ip netns exec $(ns) ");
                cmd = @"ip netns exec $(ns) ip link set dev $(pseudo_dev) up";
                print(@"$(cmd)\n");
                com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
            assert(! pseudo_macs.has_key(pseudo_dev));
            pseudo_macs[pseudo_dev] = pseudo_mac;
        }

        public void add_address(string ns, string pseudo_dev, string linklocal)
        {
            assert(ns != "");
            try {
                string cmd = @"ip netns exec $(ns) ip address add $(linklocal) dev $(pseudo_dev)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
            HandledNic n = new HandledNic();
            n.dev = pseudo_dev;
            n.mac = pseudo_macs[pseudo_dev];
            n.linklocal = linklocal;
            int linklocal_index = linklocal_nextindex++;
            linklocals[linklocal_index] = n;
            print(@"linklocals: #$(linklocal_index): $(n.dev) ($(n.mac)) has $(n.linklocal).\n");
        }

        public void add_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            // ns may be empty-string.
            try {
                string cmd = @"ip route add $(linklocal_dst) dev $(dev) src $(linklocal_src)";
                if (ns != "") cmd = @"ip netns exec $(ns) $(cmd)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void remove_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            // ns may be empty-string.
            try {
                string cmd = @"ip route del $(linklocal_dst) dev $(dev) src $(linklocal_src)";
                if (ns != "") cmd = @"ip netns exec $(ns) $(cmd)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void flush_table(string ns)
        {
            assert(ns != "");
            try {
                string cmd = @"ip netns exec $(ns) ip route flush table main";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void delete_pseudodev(string ns, string pseudo_dev)
        {
            if (pseudo_macs.has_key(pseudo_dev)) pseudo_macs.unset(pseudo_dev);
            assert(ns != "");
            try {
                string cmd = @"ip netns exec $(ns) ip link delete $(pseudo_dev) type macvlan";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void delete_namespace(string ns)
        {
            assert(ns != "");
            try {
                string cmd = @"ip netns del $(ns)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
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

            public void notify_identity_removed(IIdentityID id)
            throws StubError, DeserializeError
            {
                addr.identity_manager.notify_identity_removed(id);
            }
        }

        public IIdmgmtArc? get_arc(CallerInfo caller)
        {
            if (caller is TcpclientCallerInfo)
            {
                TcpclientCallerInfo c = (TcpclientCallerInfo)caller;
                ISourceID sourceid = c.sourceid;
                string my_address = c.my_address;
                foreach (string dev in current_nics.keys)
                {
                    HandledNic n = current_nics[dev];
                    if (n.linklocal == my_address)
                    {
                        INeighborhoodArc? neighborhood_arc = neighborhood_mgr.get_node_arc(sourceid, dev);
                        if (neighborhood_arc == null)
                        {
                            // some warning message?
                            return null;
                        }
                        foreach (int i in nodearcs.keys)
                        {
                            Arc arc = nodearcs[i];
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
                foreach (string dev in current_nics.keys)
                {
                    HandledNic n = current_nics[dev];
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
                print(@"ping -n -q -c 1 $(peer_addr)\n");
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
                        print(@" returned $(ret) microseconds.\n");
                        return ret;
                    }
                }
            }
            throw new NeighborhoodGetRttError.GENERIC(@"could not parse $(com_ret.stdout)");
        }
    }

    class NeighborhoodMissingArcHandler : Object, INeighborhoodMissingArcHandler
    {
        public NeighborhoodMissingArcHandler.from_qspn(IQspnMissingArcHandler qspn_missing, int nodeid_index)
        {
            this.qspn_missing = qspn_missing;
            this.nodeid_index = nodeid_index;
        }
        private IQspnMissingArcHandler? qspn_missing;
        private int nodeid_index;

        public void missing(INeighborhoodArc arc)
        {
            if (qspn_missing != null)
            {
                // from a INeighborhoodArc get a list of QspnArc
                foreach (QspnArc qspn_arc in nodeids[nodeid_index].my_arcs)
                    if (qspn_arc.arc.neighborhood_arc == arc)
                        qspn_missing.i_qspn_missing(qspn_arc);
            }
        }
    }

    class QspnStubFactory : Object, IQspnStubFactory
    {
        public QspnStubFactory(int nodeid_index)
        {
            this.nodeid_index = nodeid_index;
        }
        private int nodeid_index;

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

        public IQspnManagerStub
                        i_qspn_get_broadcast(
                            Gee.List<IQspnArc> arcs,
                            IQspnMissingArcHandler? missing_handler=null
                        )
        {
            assert(! arcs.is_empty);
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
                n_missing_handler = new NeighborhoodMissingArcHandler.from_qspn(missing_handler, nodeid_index);
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
        public QspnArc(Arc arc, NodeID sourceid, NodeID destid, Naddr neighbour_naddr)
        {
            this.arc = arc;
            this.sourceid = sourceid;
            this.destid = destid;
            this.neighbour_naddr = neighbour_naddr;
        }
        public weak Arc arc;
        public NodeID sourceid;
        public NodeID destid;
        public Naddr neighbour_naddr;

        public IQspnCost i_qspn_get_cost()
        {
            return new Cost(arc.cost);
        }

        public IQspnNaddr i_qspn_get_naddr()
        {
            return neighbour_naddr;
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
        foreach (int nodeid_index in nodeids.keys)
        {
            NodeID nodeid_index_id = nodeids[nodeid_index].nodeid;
            if (nodeid_index_id.equals(unicast_id))
            {
                foreach (int identityarc_index in identityarcs.keys)
                {
                    IdentityArc ia = identityarcs[identityarc_index];
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address)
                    {
                        if (ia.id.equals(nodeid_index_id))
                        {
                            if (ia.id_arc.get_peer_nodeid().equals(source_id))
                            {
                                return nodeids[nodeid_index].addr_man;
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
        foreach (int nodeid_index in nodeids.keys)
        {
            NodeID nodeid_index_id = nodeids[nodeid_index].nodeid;
            if (nodeid_index_id in broadcast_set)
            {
                foreach (int identityarc_index in identityarcs.keys)
                {
                    IdentityArc ia = identityarcs[identityarc_index];
                    IdmgmtArc __arc = (IdmgmtArc)ia.arc;
                    Arc _arc = __arc.arc;
                    if (_arc.neighborhood_arc.neighbour_nic_addr == peer_address
                        && _arc.neighborhood_arc.nic.dev == dev)
                    {
                        if (ia.id.equals(nodeid_index_id))
                        {
                            if (ia.id_arc.get_peer_nodeid().equals(source_id))
                            {
                                ret.add(nodeids[nodeid_index].addr_man);
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
        IdentityArc ia = new IdentityArc();
        ia.arc = arc;
        ia.id = id;
        ia.id_arc = id_arc;
        int identityarc_index = identityarc_nextindex++;
        identityarcs[identityarc_index] = ia;
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).\n");
        string peer_ll = ia.id_arc.get_peer_linklocal();
        string ns = identity_mgr.get_namespace(ia.id);
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"                  dev-ll: from $(pseudodev) on '$(ns)' to $(peer_ll).\n");
    }

    void identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
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
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).\n");
        string peer_ll = ia.id_arc.get_peer_linklocal();
        string ns = identity_mgr.get_namespace(ia.id);
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"                  dev-ll: from $(pseudodev) on '$(ns)' to $(peer_ll).\n");
        // I shouldn't need to change anything in 'IdentityArc ia', cause it's the same instance.
        assert(ia.id_arc == id_arc);
    }

    void identity_arc_removed(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        print("An identity-arc has been removed.\n");
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
        print(@"identityarcs: #$(identityarc_index): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
        print(@"                  id-id: from $(id.id) to $(peer_nodeid.id).\n");
        string peer_ll = ia.id_arc.get_peer_linklocal();
        string ns = identity_mgr.get_namespace(ia.id);
        string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
        print(@"                  dev-ll: from $(pseudodev) on '$(ns)' to $(peer_ll).\n");
        identityarcs.unset(identityarc_index);
        // TODO: Remove qspn arc if present.
    }

    void nic_address_set(string my_dev, string my_addr)
    {
        string my_mac = macgetter.get_mac(my_dev).up();
        HandledNic n = new HandledNic();
        n.dev = my_dev;
        n.mac = my_mac;
        n.linklocal = my_addr;
        int linklocal_index = linklocal_nextindex++;
        linklocals[linklocal_index] = n;
        print(@"linklocals: #$(linklocal_index): $(n.dev) ($(n.mac)) has $(n.linklocal).\n");
        current_nics[n.dev] = n;
        if (identity_mgr != null)
        {
            identity_mgr.add_handled_nic(n.dev, n.mac, n.linklocal);
        }
    }

    void arc_added(INeighborhoodArc arc)
    {
        print(@"arc_added for $(arc.neighbour_nic_addr)\n");
        print(@" $(arc.nic.dev) ($(arc.nic.mac) = $(current_nics[arc.nic.dev].linklocal)) connected to $(arc.neighbour_mac)\n");
        string k = @"$(arc.nic.mac)-$(arc.neighbour_mac)";
        assert(! (k in neighborhood_arcs.keys));
        neighborhood_arcs[k] = arc;
    }

    void arc_changed(INeighborhoodArc arc)
    {
        print(@"arc_changed (no effect) for $(arc.neighbour_nic_addr)\n");
    }

    void arc_removed(INeighborhoodArc arc)
    {
        print(@"arc_removed for $(arc.neighbour_nic_addr)\n");
        string k = @"$(arc.nic.mac)-$(arc.neighbour_mac)";
        neighborhood_arcs.unset(k);
        // Had this arc been added to 'nodearcs'?
        foreach (int nodearc_index in nodearcs.keys)
        {
            Arc node_arc = nodearcs[nodearc_index];
            if (arc == node_arc.neighborhood_arc)
            {
                nodearcs.unset(nodearc_index);
                identity_mgr.remove_arc(node_arc.idmgmt_arc);
                break;
            }
        }
    }

    string ip_global_node(Gee.List<int> n_addr)
    {
        // 1·2·3·4·5 in public-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        int32 ip = 0;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_anonymizing_node(Gee.List<int> n_addr)
    {
        // 1·2·3·4·5 in anon-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        int32 ip = 2;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_global_gnode(Gee.List<int> n_addr, int n_level)
    {
        // 1·2·3·* in public-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        int32 ip = 0;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c >= n_level) ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string ip_anonymizing_gnode(Gee.List<int> n_addr, int n_level)
    {
        // 1·2·3·* in anon-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        int32 ip = 2;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c >= n_level) ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string ip_internal_node(Gee.List<int> n_addr, int inside_level)
    {
        // *·3·4·5 in public-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < inside_level; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(inside_level >= 1);
        assert(inside_level < levels);
        int32 ip = 1;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c == levels - 1) ip += inside_level;
            else if (c >= inside_level) {}
            else ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_internal_gnode(Gee.List<int> n_addr, int n_level, int inside_level)
    {
        // *·3·* in public-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < inside_level; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        assert(inside_level > n_level);
        assert(inside_level < levels);
        int32 ip = 1;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c == levels - 1) ip += inside_level;
            else if (c >= inside_level) {}
            else if (c < n_level) {}
            else ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string ip_whole_network()
    {
        int sum = 0;
        for (int k = 0; k <= levels - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum - 2;
        string ret = @"10.0.0.0/$(prefix)";
        return ret;
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

    void show_linklocals()
    {
        foreach (int i in linklocals.keys)
        {
            HandledNic n = linklocals[i];
            print(@"linklocals: #$(i): $(n.dev) ($(n.mac)) has $(n.linklocal).\n");
        }
    }

    void show_nodeids()
    {
        foreach (int i in nodeids.keys)
        {
            NodeID nodeid = nodeids[i].nodeid;
            bool nodeid_ready = nodeids[i].ready;
            bool main = identity_mgr.get_main_id().equals(nodeid);
            print(@"nodeids: #$(i): $(nodeid.id), $(nodeid_ready ? "" : "not ")ready.$(main ? " [main]" : "")\n");
        }
    }

    void show_neighborhood_arcs()
    {
        foreach (string k in neighborhood_arcs.keys)
        {
            INeighborhoodArc arc = neighborhood_arcs[k];
            print(@"arc $(k) is for $(arc.neighbour_nic_addr)\n");
        }
    }

    void add_node_arc(INeighborhoodArc _arc, int cost)
    {
        // Had this arc been already added to 'nodearcs'?
        foreach (int nodearc_index in nodearcs.keys)
        {
            Arc node_arc = nodearcs[nodearc_index];
            if (_arc == node_arc.neighborhood_arc)
            {
                print("Already there.\n");
                return;
            }
        }
        Arc arc = new Arc();
        arc.cost = cost;
        arc.neighborhood_arc = _arc;
        arc.idmgmt_arc = new IdmgmtArc(arc);
        int nodearc_index = nodearc_nextindex++;
        nodearcs[nodearc_index] = arc;
        string _dev = arc.idmgmt_arc.get_dev();
        string _p_ll = arc.idmgmt_arc.get_peer_linklocal();
        string _p_mac = arc.idmgmt_arc.get_peer_mac();
        print(@"nodearcs: #$(nodearc_index): from $(_dev) to $(_p_ll) ($(_p_mac)).\n");
        identity_mgr.add_arc(arc.idmgmt_arc);
    }

    void show_nodearcs()
    {
        foreach (int i in nodearcs.keys)
        {
            Arc arc = nodearcs[i];
            string _dev = arc.idmgmt_arc.get_dev();
            string _p_ll = arc.idmgmt_arc.get_peer_linklocal();
            string _p_mac = arc.idmgmt_arc.get_peer_mac();
            print(@"nodearcs: #$(i): from $(_dev) to $(_p_ll) ($(_p_mac)).\n");
        }
    }

    void change_nodearc(int nodearc_index, int cost)
    {
        error("not implemented yet");
    }

    void remove_nodearc(int nodearc_index)
    {
        error("not implemented yet");
    }

    void show_identityarcs()
    {
        foreach (int i in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[i];
            IIdmgmtArc arc = ia.arc;
            NodeID id = ia.id;
            IIdmgmtIdentityArc id_arc = ia.id_arc;
            print(@"identityarcs: #$(i): on arc from $(arc.get_dev()) to $(arc.get_peer_mac()),\n");
            print(@"                  id-id: from $(id.id) to $(id_arc.get_peer_nodeid().id).\n");
            string peer_ll = ia.id_arc.get_peer_linklocal();
            string ns = identity_mgr.get_namespace(ia.id);
            string pseudodev = identity_mgr.get_pseudodev(ia.id, ia.arc.get_dev());
            print(@"                  dev-ll: from $(pseudodev) on '$(ns)' to $(peer_ll).\n");
        }
    }

    void show_ntkaddress(int nodeid_index)
    {
        Naddr my_naddr = nodeids[nodeid_index].my_naddr;
        Fingerprint my_fp = nodeids[nodeid_index].my_fp;
        string my_naddr_str = naddr_repr(my_naddr);
        string my_elderships_str = fp_elderships_repr(my_fp);
        print(@"my_naddr = $(my_naddr_str), elderships = $(my_elderships_str), fingerprint = $(my_fp.id).\n");
    }

    void prepare_add_identity(int migration_id, int old_nodeid_index)
    {
        NodeID old_id = nodeids[old_nodeid_index].nodeid;
        identity_mgr.prepare_add_identity(migration_id, old_id);
    }

    void add_identity(int migration_id, int old_nodeid_index)
    {
        NodeID old_id = nodeids[old_nodeid_index].nodeid;
        NodeID new_id = identity_mgr.add_identity(migration_id, old_id);
        int nodeid_index = nodeid_nextindex++;
        nodeids[nodeid_index] = new IdentityData(new_id);
        nodeids[nodeid_index].nodeid_index = nodeid_index;
        nodeids[old_nodeid_index].main_id = false;

        string new_ns = identity_mgr.get_namespace(old_id);
        string old_ns = identity_mgr.get_namespace(new_id);
        nodeids[nodeid_index].main_id = (old_ns == "");
        LinuxRoute new_route = new LinuxRoute(new_ns, ip_whole_network());
        LinuxRoute old_route = nodeids[old_nodeid_index].route;
        nodeids[old_nodeid_index].route = new_route;
        nodeids[nodeid_index].route = old_route;

        print(@"nodeids: #$(nodeid_index): $(new_id.id).\n");
    }

    void enter_net
    (int new_nodeid_index,
     int previous_nodeid_index,
     string s_naddr_new_gnode,
     string s_elderships_new_gnode,
     int hooking_gnode_level,
     int into_gnode_level,
     Gee.List<int> idarc_index_set,
     Gee.List<string> idarc_address_set)
    {
        IdentityData previous_identity = nodeids[previous_nodeid_index];
        IdentityData new_identity = nodeids[new_nodeid_index];
        NodeID new_id = new_identity.nodeid;
        NodeID previous_id = previous_identity.nodeid;
        QspnManager previous_id_mgr = (QspnManager)identity_mgr.get_identity_module(previous_id, "qspn");
        Naddr previous_id_my_naddr = previous_identity.my_naddr;
        Fingerprint previous_id_my_fp = previous_identity.my_fp;
        LinuxRoute new_id_route = new_identity.route;

        if (previous_id_mgr.is_bootstrap_complete())
        {
            Gee.List<HCoord> dests;
            try {
                dests = previous_id_mgr.get_known_destinations();
            } catch (QspnBootstrapInProgressError e) {assert_not_reached();}
            foreach (HCoord h in dests)
            {
                if (h.pos >= _gsizes[h.lvl]) continue; // ignore virtual destination.

                // Compute Netsukuku address of `h`.
                ArrayList<int> h_addr = new ArrayList<int>();
                h_addr.add_all(previous_identity.my_naddr.pos);
                h_addr[h.lvl] = h.pos;
                for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

                // Remove routes towards global IPs. Then, remove routes towards internal IPs
                //  only inside lvl > hooking_gnode_level.
                // Operations now are based on type of previous_identity:
                // Is this the main ID? Do I have a *real* Netsukuku address?
                int real_up_to = previous_identity.my_naddr.get_real_up_to();
                int virtual_up_to = previous_identity.my_naddr.get_virtual_up_to();
                if (previous_identity.main_id)
                {
                    if (real_up_to == levels-1)
                    {
                        // Global.
                        new_id_route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                        // Anonymizing.
                        new_id_route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                        // Internals. In this case they are guaranteed to be valid.
                        for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
                        {
                            new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else
                    {
                        if (h.lvl <= real_up_to)
                        {
                            // Internals. In this case they MUST be checked.
                            bool invalid_found = false;
                            for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
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
                                new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                            }
                        }
                        else if (h.lvl < virtual_up_to)
                        {

                            // Internals. In this case they MUST be checked.
                            bool invalid_found = false;
                            for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
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
                                new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                            }
                        }
                        else
                        {
                            // Global.
                            new_id_route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                            // Anonymizing.
                            new_id_route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                            // Internals. In this case they are guaranteed to be valid.
                            for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
                            {
                                new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                            }
                        } 
                    }
                }
                else
                {
                    if (h.lvl < virtual_up_to)
                    {
                        // Internals. In this case they MUST be checked.
                        bool invalid_found = false;
                        for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
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
                            new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    }
                    else
                    {
                        // Global.
                        new_id_route.remove_destination(ip_global_gnode(h_addr, h.lvl));
                        // Anonymizing.
                        new_id_route.remove_destination(ip_anonymizing_gnode(h_addr, h.lvl));
                        // Internals. In this case they are guaranteed to be valid.
                        for (int t = h.lvl + 1; t <= levels - 1; t++) if (t > hooking_gnode_level)
                        {
                            new_id_route.remove_destination(ip_internal_gnode(h_addr, h.lvl, t));
                        }
                    } 
                }
            }
        }

        // Remove my global IP. Then, remove my internal IPs only inside lvl > into_gnode_level-1.
        // Operations now are based on type of previous_identity:
        // Is this the main ID? Do I have a *real* Netsukuku address?
        if (previous_identity.main_id)
        {
            // Do I have a *real* Netsukuku address?
            int real_up_to = previous_identity.my_naddr.get_real_up_to();
            if (real_up_to == levels-1)
            {
                string ip_global = ip_global_node(previous_identity.my_naddr.pos);
                foreach (string dev in real_nics)
                    new_id_route.remove_address(ip_global, dev);
                if (accept_anonymous_requests)
                {
                    string ip_anonymizing = ip_anonymizing_node(previous_identity.my_naddr.pos);
                    foreach (string dev in real_nics)
                        new_id_route.remove_address(ip_anonymizing, dev);
                }
            }
            for (int j = 0; j <= levels-2 && j <= real_up_to; j++) if (j > into_gnode_level-1)
            {
                string ip_internal = ip_internal_node(previous_identity.my_naddr.pos, j+1);
                foreach (string dev in real_nics)
                    new_id_route.remove_address(ip_internal, dev);
            }
        }

        ArrayList<int> _naddr = new ArrayList<int>();
        ArrayList<int> _elderships = new ArrayList<int>();
        foreach (string s_piece in s_naddr_new_gnode.split(".")) _naddr.insert(0, int.parse(s_piece));
        foreach (string s_piece in s_elderships_new_gnode.split(".")) _elderships.insert(0, int.parse(s_piece));
        if (_naddr.size != _elderships.size) error("You have to use same number of levels");
        int level_new_gnode = levels - _naddr.size;
        assert(into_gnode_level > level_new_gnode);
        assert(level_new_gnode >= hooking_gnode_level);
        for (int i = level_new_gnode-1; i >= 0; i--)
        {
            int pos = previous_id_my_naddr.pos[i];
            int eldership = previous_id_my_fp.elderships[i];
            _naddr.insert(0, pos);
            if (i >= hooking_gnode_level)
                _elderships.insert(0, 0);
            else
                _elderships.insert(0, eldership);
        }
        Naddr my_naddr = new Naddr(_naddr.to_array(), _gsizes.to_array());
        Fingerprint my_fp = new Fingerprint(_elderships.to_array(), previous_id_my_fp.id);
        string my_naddr_str = naddr_repr(my_naddr);
        string my_elderships_str = fp_elderships_repr(my_fp);
        print(@"new identity will be $(my_naddr_str), elderships = $(my_elderships_str), fingerprint = $(my_fp.id).\n");
        ArrayList<IQspnArc> my_arcs = new ArrayList<IQspnArc>((a, b) => a.i_qspn_equals(b));
        assert(idarc_index_set.size == idarc_address_set.size);
        for (int i = 0; i < idarc_index_set.size; i++)
        {
            int idarc_index = idarc_index_set[i];
            string idarc_address = idarc_address_set[i];
            ArrayList<int> idarc_naddr = new ArrayList<int>();
            foreach (string s_piece in idarc_address.split(".")) idarc_naddr.insert(0, int.parse(s_piece));
            Naddr neighbour_naddr = new Naddr(idarc_naddr.to_array(), _gsizes.to_array());
            assert(idarc_index in identityarcs.keys);
            IdentityArc ia = identityarcs[idarc_index];
            NodeID destid = ia.id_arc.get_peer_nodeid();
            NodeID sourceid = ia.id;
            IdmgmtArc __arc = (IdmgmtArc)ia.arc;
            Arc _arc = __arc.arc;
            QspnArc arc = new QspnArc(_arc, sourceid, destid, neighbour_naddr);
            my_arcs.add(arc);
            new_id_route.add_neighbour(ia.id_arc.get_peer_mac());
        }
        QspnManager qspn_mgr = new QspnManager.enter_net(my_naddr,
            my_arcs,
            my_fp,
            new QspnStubFactory(new_nodeid_index),
            hooking_gnode_level,
            into_gnode_level,
            previous_id_mgr);
        identity_mgr.set_identity_module(new_id, "qspn", qspn_mgr);
        new_identity.my_naddr = my_naddr;
        new_identity.my_fp = my_fp;
        new_identity.ready = true;
        new_identity.addr_man = new AddressManagerForIdentity(qspn_mgr);
        new_identity.my_arcs.add_all(my_arcs);

        ArrayList<string> pseudodevs = new ArrayList<string>();
        foreach (string real_nic in real_nics) pseudodevs.add(identity_mgr.get_pseudodev(new_id, real_nic));
        if (/* Is this the main ID? */ new_identity.main_id)
        {
            // Do I have a *real* Netsukuku address?
            int real_up_to = my_naddr.get_real_up_to();
            if (real_up_to == levels-1)
            {
                new_identity.ip_global = ip_global_node(my_naddr.pos);
                foreach (string dev in pseudodevs) new_id_route.add_address(new_identity.ip_global, dev);
                if (accept_anonymous_requests)
                {
                    new_identity.ip_anonymizing = ip_anonymizing_node(my_naddr.pos);
                    foreach (string dev in pseudodevs) new_id_route.add_address(new_identity.ip_anonymizing, dev);
                }
            }
            new_identity.ip_internal = new ArrayList<string>();
            for (int j = 0; j <= levels-2 && j <= real_up_to; j++)
            {
                new_identity.ip_internal.add(ip_internal_node(my_naddr.pos, j+1));
                foreach (string dev in pseudodevs) new_id_route.add_address(new_identity.ip_internal[j], dev);
            }
        }

        qspn_mgr.arc_removed.connect(new_identity.arc_removed);
        qspn_mgr.changed_fp.connect(new_identity.changed_fp);
        qspn_mgr.changed_nodes_inside.connect(new_identity.changed_nodes_inside);
        qspn_mgr.destination_added.connect(new_identity.destination_added);
        qspn_mgr.destination_removed.connect(new_identity.destination_removed);
        qspn_mgr.gnode_splitted.connect(new_identity.gnode_splitted);
        qspn_mgr.path_added.connect(new_identity.path_added);
        qspn_mgr.path_changed.connect(new_identity.path_changed);
        qspn_mgr.path_removed.connect(new_identity.path_removed);
        qspn_mgr.presence_notified.connect(new_identity.presence_notified);
        qspn_mgr.qspn_bootstrap_complete.connect(new_identity.qspn_bootstrap_complete);
        qspn_mgr.remove_identity.connect(new_identity.remove_identity);
    }

    void add_qspnarc(int nodeid_index, int idarc_index, string idarc_address)
    {
        NodeID id = nodeids[nodeid_index].nodeid;
        QspnManager id_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");

        ArrayList<int> idarc_naddr = new ArrayList<int>();
        foreach (string s_piece in idarc_address.split(".")) idarc_naddr.insert(0, int.parse(s_piece));
        Naddr neighbour_naddr = new Naddr(idarc_naddr.to_array(), _gsizes.to_array());
        assert(idarc_index in identityarcs.keys);
        IdentityArc ia = identityarcs[idarc_index];
        NodeID destid = ia.id_arc.get_peer_nodeid();
        NodeID sourceid = ia.id;
        IdmgmtArc __arc = (IdmgmtArc)ia.arc;
        Arc _arc = __arc.arc;
        QspnArc arc = new QspnArc(_arc, sourceid, destid, neighbour_naddr);
        id_mgr.arc_add(arc);
        nodeids[nodeid_index].my_arcs.add(arc);
        nodeids[nodeid_index].route.add_neighbour(ia.id_arc.get_peer_mac());
    }
}

