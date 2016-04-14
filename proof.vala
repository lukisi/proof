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

    string naddr;
    string gsizes;
    [CCode (array_length = false, array_null_terminated = true)]
    string[] interfaces;
    bool accept_anonymous_requests;
    bool no_anonymize;

    ITasklet tasklet;
    Netsukuku.Neighborhood.NeighborhoodManager? neighborhood_mgr;
    Netsukuku.Identities.
    IdentityManager? identity_mgr;
    int linklocal_nextindex;
    HashMap<int, HandledNic> linklocals;

    IAddressManagerSkeleton node_skeleton;
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
        ArrayList<int> _gsizes = new ArrayList<int>();
        ArrayList<string> _devs = new ArrayList<string>();
        foreach (string s_piece in naddr.split(".")) _naddr.insert(0, int.parse(s_piece));
        foreach (string s_piece in gsizes.split(".")) _gsizes.insert(0, int.parse(s_piece));
        foreach (string dev in interfaces) _devs.add(dev);
        if (_naddr.size != _gsizes.size) error("You have to use same number of levels");

        // Initialize tasklet system
        PthTaskletImplementer.init();
        tasklet = PthTaskletImplementer.get_tasklet_system();

        // Initialize known serializable classes
        // typeof(MyNodeID).class_peek();

        // TODO startup

        prepare_all_nics();
        node_skeleton = new AddressManagerForNode();
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

        // Init module Neighborhood
        NeighborhoodManager.init(tasklet);
        identity_mgr = null;
        linklocals = new HashMap<int, HandledNic>();
        neighborhood_mgr = new NeighborhoodManager(
            get_identity_skeleton,
            get_identity_skeleton_set,
            node_skeleton,
            1000 /*very high max_arcs*/,
            new NeighborhoodStubFactory(),
            new NeighborhoodIPRouteManager());
        // connect signals
        neighborhood_mgr.nic_address_set.connect(nic_address_set);
        foreach (string dev in _devs) manage_nic(dev);
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
        identity_mgr.identity_arc_added.connect(identity_arc_added);
        identity_mgr.identity_arc_changed.connect(identity_arc_changed);
        identity_mgr.identity_arc_removed.connect(identity_arc_removed);

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

        // This will destroy the object NeighborhoodManager and hence call
        //  its stop_monitor_all.
        // Beware that node_skeleton.neighborhood_mgr
        //  is a weak reference.
        // Beware also that since we destroy the object, we won't receive
        //  any more signal from it, such as nic_address_unset for all the
        //  linklocal addresses that will be removed from the NICs.
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

    void manage_nic(string dev)
    {
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
                error(@"$(com_ret.stderr)\n");
            com_ret = tasklet.exec_command(@"$(ns_prefix)sysctl -n $(key)");
            if (com_ret.exit_status != 0)
                error(@"$(com_ret.stderr)\n");
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
                    else if (_args[0] == "quit" && _args.size == 1)
                    {
                        do_me_exit = true;
                    }
                    else if (_args[0] == "show_linklocals" && _args.size == 1)
                    {
                        show_linklocals();
                    }
                    else if (_args[0] == "help" && _args.size == 1)
                    {
                        print("""
Command list:

> show_linklocals
  List current link-local addresses

> help
  Shows this menu.

> quit
  Exits. You can also press <ctrl-C>.

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

    class NeighborhoodIPRouteManager : Object, INeighborhoodIPRouteManager
    {
        public void add_address(string my_addr, string my_dev)
        {
            try {
                TaskletCommandResult com_ret = tasklet.exec_command(@"ip address add $(my_addr) dev $(my_dev)");
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void add_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            error("not implemented yet");
        }

        public void remove_address(string my_addr, string my_dev)
        {
            try {
                TaskletCommandResult com_ret = tasklet.exec_command(@"ip address del $(my_addr)/32 dev $(my_dev)");
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error(@"Unable to spawn a command: $(e.message)");}
        }

        public void remove_neighbor(string my_addr, string my_dev, string neighbor_addr)
        {
            error("not implemented yet");
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
        get_tcp(
            string dest,
            ISourceID source_id,
            IUnicastID unicast_id,
            bool wait_reply = true)
        {
            error("not implemented yet");
        }

        public IAddressManagerStub
        get_unicast(
            string dev,
            string src_ip,
            ISourceID source_id,
            IUnicastID unicast_id,
            bool wait_reply = true)
        {
            error("not implemented yet");
        }
    }

    class IdmgmtNetnsManager : Object, IIdmgmtNetnsManager
    {
        public void add_address(string ns, string pseudo_dev, string linklocal)
        {
            error("not implemented yet");
        }

        public void add_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            error("not implemented yet");
        }

        public void create_namespace(string ns)
        {
            error("not implemented yet");
        }

        public void create_pseudodev(string dev, string ns, string pseudo_dev, out string pseudo_mac)
        {
            error("not implemented yet");
        }

        public void delete_namespace(string ns)
        {
            error("not implemented yet");
        }

        public void delete_pseudodev(string ns, string pseudo_dev)
        {
            error("not implemented yet");
        }

        public void flush_table(string ns)
        {
            error("not implemented yet");
        }

        public void remove_gateway(string ns, string linklocal_src, string linklocal_dst, string dev)
        {
            error("not implemented yet");
        }
    }

    class IdmgmtStubFactory : Object, IIdmgmtStubFactory
    {
        public IIdmgmtArc? get_arc(CallerInfo caller)
        {
            error("not implemented yet");
        }

        public IIdentityManagerStub get_stub(IIdmgmtArc arc)
        {
            error("not implemented yet");
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
            error("not implemented yet");
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
        public unowned INeighborhoodManagerSkeleton
        neighborhood_manager_getter()
        {
            error("AddressManagerForIdentity.neighborhood_manager_getter: not for identity");
        }

        protected unowned IIdentityManagerSkeleton
        identity_manager_getter()
        {
            error("AddressManagerForIdentity.identity_manager_getter: not for identity");
        }

        public unowned IQspnManagerSkeleton
        qspn_manager_getter()
        {
            error("not implemented yet");
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
        public unowned INeighborhoodManagerSkeleton
        neighborhood_manager_getter()
        {
            error("not implemented yet");
        }

        protected unowned IIdentityManagerSkeleton
        identity_manager_getter()
        {
            error("not implemented yet");
        }

        public unowned IQspnManagerSkeleton
        qspn_manager_getter()
        {
            error("AddressManagerForNode.qspn_manager_getter: not for node");
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
        error("not implemented yet");
    }

    Gee.List<IAddressManagerSkeleton>
    get_identity_skeleton_set(
        NodeID source_id,
        Gee.List<NodeID> broadcast_set,
        string peer_address,
        string dev)
    {
        error("not implemented yet");
    }

    void identity_arc_added(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
        error("not implemented yet");
    }

    void identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
        error("not implemented yet");
    }

    void identity_arc_removed(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        error("not implemented yet");
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
        if (identity_mgr != null)
        {
            identity_mgr.add_handled_nic(n.dev, n.mac, n.linklocal);
        }
    }

    void show_linklocals()
    {
        foreach (int i in linklocals.keys)
        {
            HandledNic n = linklocals[i];
            print(@"linklocals: #$(i): $(n.dev) ($(n.mac)) has $(n.linklocal).\n");
        }
    }
}

