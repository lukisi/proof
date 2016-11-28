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
    const string help_commands = """
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

> add_qspn_arc <local_identity_index> <peer_mac>
  Add a QspnArc.

> check_connectivity <local_identity_index>
  Checks whether a connectivity identity is still necessary.

> help
  Show this menu.

> quit
  Exit.

""";

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
                        string k = key_for_physical_arc(_args[1].up(), _args[2].up());
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
                        string k = key_for_physical_arc(_args[1].up(), _args[2].up());
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
                        string k = key_for_physical_arc(_args[1].up(), _args[2].up());
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
                        string host_gnode_elderships = _args[5];
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
                        ArrayList<int> id_arc_index_list = new ArrayList<int>();
                        foreach (string s_piece in id_arc_index_list_str.split(",")) id_arc_index_list.add(int.parse(s_piece));
                        int op_id = int.parse(_args[13]);
                        string prev_op_id_str = _args[14];
                        int? prev_op_id = null;
                        if (prev_op_id_str != "null") prev_op_id = int.parse(prev_op_id_str);
                        prepare_enter_net_phase_1(
                            local_identity_index,
                            guest_gnode_level,
                            host_gnode_level,
                            host_gnode_address,
                            host_gnode_elderships,
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
                    else if (_args[0] == "add_qspn_arc")
                    {
                        if (_args.size != 3)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        string peer_mac = _args[2].up();
                        add_qspn_arc(local_identity_index, peer_mac);
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
        IdentityData identity_data = local_identities[index];
        string my_naddr_str = naddr_repr(identity_data.my_naddr);
        string my_elderships_str = fp_elderships_repr(identity_data.my_fp);
        string my_fp0 = @"$(identity_data.my_fp.id)";
        string l0 = @"local_identity #$(index):";
        l0 += @" address $(my_naddr_str), elderships $(my_elderships_str),";
        string network_namespace_str = identity_data.network_namespace;
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
            IdentityData identity_data = find_or_create_local_identity(id);
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
        string host_gnode_elderships,
        int in_host_pos1,
        int in_host_pos1_eldership,
        int in_host_pos2,
        int in_host_pos2_eldership,
        int connectivity_pos,
        int connectivity_pos_eldership,
        ArrayList<int> id_arc_index_list,
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
        pending.host_gnode_elderships = host_gnode_elderships;
        pending.in_host_pos1 = in_host_pos1;
        pending.in_host_pos1_eldership = in_host_pos1_eldership;
        pending.in_host_pos2 = in_host_pos2;
        pending.in_host_pos2_eldership = in_host_pos2_eldership;
        pending.connectivity_pos = connectivity_pos;
        pending.connectivity_pos_eldership = connectivity_pos_eldership;
        pending.id_arc_index_list = new ArrayList<int>();
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
        public string host_gnode_elderships;
        public int in_host_pos1;
        public int in_host_pos1_eldership;
        public int in_host_pos2;
        public int in_host_pos2_eldership;
        public int connectivity_pos;
        public int connectivity_pos_eldership;
        public ArrayList<int> id_arc_index_list;
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
        IdentityData new_identity_data = find_or_create_local_identity(new_id);
        new_identity_data.copy_of_identity = old_identity_data;
        new_identity_data.connectivity_from_level = old_identity_data.connectivity_from_level;
        new_identity_data.connectivity_to_level = old_identity_data.connectivity_to_level;

        old_identity_data.connectivity_from_level = op.guest_gnode_level + 1;
        old_identity_data.connectivity_to_level = levels - 1;
        // Anyway, in the scenario of enter_net, there is no need to call `make_connectivity` on
        //  the QspnManager of old_identity_data.

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

        // Move routes of old identity into new network namespace

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
            // Note: Member peer_mac is not changed yet. It is the old one.
            // Whilst ia.id_arc.get_peer_mac() might differ. If it was a g-node migration that includes this neighbor.
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

        // Add routes of new identity into old network namespace

        // new address = op.host_gnode_address + op.in_host_pos1
        //             + old_identity_data.my_naddr.pos.slice(0, op.host_gnode_level-1)
        ArrayList<int> _naddr_new = new ArrayList<int>();
        foreach (string s_piece in op.host_gnode_address.split(".")) _naddr_new.insert(0, int.parse(s_piece));
        _naddr_new.insert(0, op.in_host_pos1);
        _naddr_new.insert_all(0, old_identity_data.my_naddr.pos.slice(0, op.host_gnode_level-1));
        Naddr my_naddr_new = new Naddr(_naddr_new.to_array(), _gsizes.to_array());
        compute_destination_ip_set(new_identity_data.destination_ip_set, my_naddr_new);

        // Add new destination IPs into all tables in old network namespace
        bid = cm.begin_block();
        tablenames = new ArrayList<string>();
        if (old_ns == "") tablenames.add("ntk");
        // Add a table for each qspn-arc of old identity that will be also in new identity
        foreach (IdentityArc ia in old_identity_data.my_identityarcs) if (ia.qspn_arc != null)
        {
            // Note: Member peer_mac is not changed yet. It is the old one.
            // Whilst ia.id_arc.get_peer_mac() might differ. If it was a g-node migration that includes this neighbor.
            // Hence, if they differ we have to use it.
            if (ia.peer_mac != ia.id_arc.get_peer_mac())
            {
                int tid;
                string tablename;
                tn.get_table(ia.peer_mac, out tid, out tablename);
                tablenames.add(tablename);
            }
        }
        foreach (string tablename in tablenames)
         for (int i = levels-1; i >= subnetlevel; i--)
         for (int j = 0; j < _gsizes[i]; j++)
        {
            if (new_identity_data.destination_ip_set[i][j].global != "")
            {
                string ipaddr = new_identity_data.destination_ip_set[i][j].global;
                ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                cm.single_command_in_block(bid, cmd);
                ipaddr = new_identity_data.destination_ip_set[i][j].anonymous;
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                cm.single_command_in_block(bid, cmd);
            }
            for (int k = levels-1; k >= i+1; k--)
            {
                if (new_identity_data.destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = new_identity_data.destination_ip_set[i][j].intern[k];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                }
            }
        }
        cm.end_block(bid);

        // New qspn manager
        ArrayList<IQspnArc> internal_arc_set = new ArrayList<IQspnArc>();
        ArrayList<IQspnNaddr> internal_arc_peer_naddr_set = new ArrayList<IQspnNaddr>();
        foreach (IdentityArc w0 in old_identity_data.my_identityarcs)
        {
            if (w0.peer_mac != w0.id_arc.get_peer_mac())
            {
                // It is an internal arc
                IdentityArc w1 = old_to_new_id_arc[w0]; // w1 is already in new_identity_data.my_identityarcs
                NodeID destid = w1.id_arc.get_peer_nodeid();
                NodeID sourceid = w1.id; // == new_id
                IdmgmtArc __arc = (IdmgmtArc)w1.arc;
                Arc _arc = __arc.arc;
                string peer_mac = w1.id_arc.get_peer_mac();
                w1.qspn_arc = new QspnArc(_arc, sourceid, destid, peer_mac);
                internal_arc_set.add(w1.qspn_arc);
                Naddr w1_peer_naddr = null;
                // TODO get peer_naddr of w0 and transform into the one of w1.
                internal_arc_peer_naddr_set.add(w1_peer_naddr);
            }
        }
        ArrayList<IQspnArc> external_arc_set = new ArrayList<IQspnArc>();
        foreach (int w0_index in op.id_arc_index_list)
        {
            IdentityArc w0 = identityarcs[w0_index];
            IdentityArc w1 = old_to_new_id_arc[w0]; // w1 is already in new_identity_data.my_identityarcs
            NodeID destid = w1.id_arc.get_peer_nodeid();
            NodeID sourceid = w1.id; // == new_id
            IdmgmtArc __arc = (IdmgmtArc)w1.arc;
            Arc _arc = __arc.arc;
            string peer_mac = w1.id_arc.get_peer_mac();
            w1.qspn_arc = new QspnArc(_arc, sourceid, destid, peer_mac);
            external_arc_set.add(w1.qspn_arc);
        }
        QspnManager.PreviousArcToNewArcDelegate old_arc_to_new_arc = (/*IQspnArc*/ old_arc) => {
            // return IQspnArc or null.
            foreach (IdentityArc old_identity_arc in old_to_new_id_arc.keys)
            {
                if (old_identity_arc.qspn_arc == old_arc)
                    return old_to_new_id_arc[old_identity_arc].qspn_arc;
            }
            return null;
        };

        // new elderships = op.host_gnode_elderships + op.in_host_pos1_eldership
        //                + 0 * (op.host_gnode_level - 1 - op.guest_gnode_level)
        //                + old_identity_data.my_fp.elderships.slice(0, op.guest_gnode_level)
        ArrayList<int> _elderships = new ArrayList<int>();
        foreach (string s_piece in op.host_gnode_elderships.split(".")) _elderships.insert(0, int.parse(s_piece));
        _elderships.insert(0, op.in_host_pos1_eldership);
        for (int jj = 0; jj < op.host_gnode_level - 1 - op.guest_gnode_level; jj++) _elderships.insert(0, 0);
        _elderships.insert_all(0, old_identity_data.my_fp.elderships.slice(0, op.guest_gnode_level));
        Fingerprint my_fp_new = new Fingerprint(_elderships.to_array(), old_identity_data.my_fp.id);
        QspnManager qspn_mgr = new QspnManager.enter_net(
            my_naddr_new,
            internal_arc_set,
            internal_arc_peer_naddr_set,
            external_arc_set,
            old_arc_to_new_arc,
            my_fp_new,
            new QspnStubFactory(new_identity_data),
            /*hooking_gnode_level*/ op.guest_gnode_level,
            /*into_gnode_level*/ op.host_gnode_level,
            /*previous_identity*/ (QspnManager)(identity_mgr.get_identity_module(old_id, "qspn")));
        identity_mgr.set_identity_module(new_id, "qspn", qspn_mgr);
        new_identity_data.my_naddr = my_naddr_new;
        new_identity_data.my_fp = my_fp_new;
        new_identity_data.ready = false;
        new_identity_data.addr_man = new AddressManagerForIdentity(qspn_mgr);

        qspn_mgr.arc_removed.connect(new_identity_data.arc_removed);
        qspn_mgr.changed_fp.connect(new_identity_data.changed_fp);
        qspn_mgr.changed_nodes_inside.connect(new_identity_data.changed_nodes_inside);
        qspn_mgr.destination_added.connect(new_identity_data.destination_added);
        qspn_mgr.destination_removed.connect(new_identity_data.destination_removed);
        qspn_mgr.gnode_splitted.connect(new_identity_data.gnode_splitted);
        qspn_mgr.path_added.connect(new_identity_data.path_added);
        qspn_mgr.path_changed.connect(new_identity_data.path_changed);
        qspn_mgr.path_removed.connect(new_identity_data.path_removed);
        qspn_mgr.presence_notified.connect(new_identity_data.presence_notified);
        qspn_mgr.qspn_bootstrap_complete.connect(new_identity_data.qspn_bootstrap_complete);
        qspn_mgr.remove_identity.connect(new_identity_data.remove_identity);
        // TODO qspn_mgr.etp_executed.connect(new_identity_data.etp_executed);

        // Add new destination IPs into new forwarding-tables in old network namespace
        bid = cm.begin_block();
        foreach (IdentityArc ia in new_identity_data.my_identityarcs)
         if (ia.qspn_arc != null)
         if (ia.qspn_arc in external_arc_set)
        {
            string mac = ia.peer_mac; // the value is up to date in the IdentityArc of new identity.
            int tid;
            string tablename;
            tn.get_table(ia.peer_mac, out tid, out tablename);

            ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                @"-m", @"mac", @"--mac-source", @"$mac",
                @"-j", @"MARK", @"--set-mark", @"$tid"});
            cm.single_command_in_block(bid, cmd);

            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (new_identity_data.destination_ip_set[i][j].global != "")
                {
                    string ipaddr = new_identity_data.destination_ip_set[i][j].global;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                    ipaddr = new_identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (new_identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        string ipaddr = new_identity_data.destination_ip_set[i][j].intern[k];
                        cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                        cm.single_command_in_block(bid, cmd);
                    }
                }
            }
        }
        cm.end_block(bid);

        // Netsukuku Address of new identity will be changing.
        if (op.prev_op_id == null)
        {
            // Immediately change address of new identity.
            int ch_level = op.host_gnode_level-1;
            int ch_pos = op.in_host_pos2;
            int ch_eldership = op.in_host_pos2_eldership;

            ArrayList<int> _naddr_new_2 = new ArrayList<int>();
            _naddr_new_2.add_all(my_naddr_new.pos);
            _naddr_new_2[ch_level] = ch_pos;
            Naddr my_naddr_new_2 = new Naddr(_naddr_new_2.to_array(), _gsizes.to_array());
            new_identity_data.my_naddr = my_naddr_new_2;

            ArrayList<int> _elderships_2 = new ArrayList<int>();
            _elderships_2.add_all(my_fp_new.elderships);
            _elderships_2[ch_level] = ch_eldership;
            Fingerprint my_fp_new_2 = new Fingerprint(_elderships_2.to_array(), my_fp_new.id);

            qspn_mgr.make_real(my_naddr_new_2);
            // TODO Method `make_real` must change also elderships of fingerprint

            // tablenames = set of tables in old network namespace
            HashMap<int,HashMap<int,DestinationIPSet>> prev_new_identity_destination_ip_set;
            prev_new_identity_destination_ip_set = copy_destination_ip_set(new_identity_data.destination_ip_set);
            compute_destination_ip_set(new_identity_data.destination_ip_set, my_naddr_new_2);
            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (new_identity_data.destination_ip_set[i][j].global != "" &&
                    prev_new_identity_destination_ip_set[i][j].global == "")
                {
                    // TODO add route and change it (foreach tablename in tablenames)
                    // TODO same for new_identity_data.destination_ip_set[i][j].anonymous
                }
                else if (new_identity_data.destination_ip_set[i][j].global == "" &&
                    prev_new_identity_destination_ip_set[i][j].global != "")
                {
                    // TODO delete route (foreach tablename in tablenames)
                    // TODO same for new_identity_data.destination_ip_set[i][j].anonymous
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (new_identity_data.destination_ip_set[i][j].intern[k] != "" &&
                        new_identity_data.destination_ip_set[i][j].intern[k] == "")
                    {
                        // TODO add route and change it (foreach tablename in tablenames)
                    }
                    else if (new_identity_data.destination_ip_set[i][j].intern[k] == "" &&
                        new_identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        // TODO delete route (foreach tablename in tablenames)
                    }
                }
            }

            if (old_ns == "")
            {
                // TODO aggiungi indirizzi IP locali (interni, globale, anon)
                // TODO aggiungi regola SNAT
                // TODO update table ntk
            }
        }

        // Finally, the peer_mac and peer_linklocal of the identity-arcs of old identity
        // can be updated (if they need to).
        foreach (IdentityArc ia in old_identity_data.my_identityarcs) if (ia.qspn_arc != null)
        {
            ia.peer_mac = ia.id_arc.get_peer_mac();
            ia.peer_linklocal = ia.id_arc.get_peer_linklocal();
        }

        // TODO Remove old identity

        error("not implemented yet");
    }

    void add_qspn_arc(int local_identity_index, string peer_mac)
    {
        IdentityData identity_data = local_identities[local_identity_index];
        NodeID sourceid = identity_data.nodeid;
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(sourceid, "qspn");

        foreach (IdentityArc ia in identity_data.my_identityarcs)
         if (ia.peer_mac == peer_mac)
        {
            NodeID destid = ia.id_arc.get_peer_nodeid();
            IdmgmtArc _arc = (IdmgmtArc)ia.arc;
            Arc arc = _arc.arc;
            ia.qspn_arc = new QspnArc(arc, sourceid, destid, peer_mac);
            qspn_mgr.arc_add(ia.qspn_arc);
            int tid;
            string tablename;
            tn.get_table(peer_mac, out tid, out tablename);

            // Add new forwarding-table
            int bid = cm.begin_block();
            string ns = identity_data.network_namespace;
            ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
            if (ns != "") prefix_cmd_ns.add_all_array({
                @"ip", @"netns", @"exec", @"$ns"});
            ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                @"-m", @"mac", @"--mac-source", @"$peer_mac",
                @"-j", @"MARK", @"--set-mark", @"$tid"});
            cm.single_command_in_block(bid, cmd);

            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (identity_data.destination_ip_set[i][j].global != "")
                {
                    string ipaddr = identity_data.destination_ip_set[i][j].global;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                    ipaddr = identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                    cm.single_command_in_block(bid, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        string ipaddr = identity_data.destination_ip_set[i][j].intern[k];
                        cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$ipaddr", @"table", @"$tablename"});
                        cm.single_command_in_block(bid, cmd);
                    }
                }
            }
            cm.end_block(bid);

            print(@"Debug: IdentityData #$(identity_data.local_identity_index): call update_all_destinations for add_qspn_arc.\n");
            update_best_paths_forall_destinations_per_identity(identity_data);
            print(@"Debug: IdentityData #$(identity_data.local_identity_index): done update_all_destinations for add_qspn_arc.\n");
        }
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
