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

> show_identity_arcs <local_identity_index>
  List current identity-arcs of a given local identity.

> update_all_routes <local_identity_index>
  Update lookup-tables of this identity based on its knowledge.

> prepare_enter_net_phase_1
  Prepare ...

> enter_net_phase_1
  Finalize ...

> add_qspn_arc <local_identity_index> <my_dev> <peer_mac>
  Add a QspnArc.

> show_qspn_data <local_identity_index>
  Shows info from QspnManager of a given local identity.

> check_connectivity <local_identity_index>
  Checks whether a connectivity identity is still necessary.

> print_time_ruler ON/OFF
  Useful during debugging.

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
                ITaskletHandle? h_time_ruler = null;
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
                    else if (_args[0] == "show_identity_arcs")
                    {
                        if (_args.size != 2)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        write_block_response(command_id, show_identity_arcs(local_identity_index));
                    }
                    else if (_args[0] == "update_all_routes")
                    {
                        if (_args.size != 2)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        update_all_routes(local_identity_index);
                        write_empty_response(command_id);
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
                        if (_args.size != 4)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        int local_identity_index = int.parse(_args[1]);
                        string my_dev = _args[2];
                        string peer_mac = _args[3].up();
                        add_qspn_arc(local_identity_index, my_dev, peer_mac);
                        write_empty_response(command_id);
                    }
                    else if (_args[0] == "show_qspn_data")
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
                        write_block_response(command_id, show_qspn_data(local_identity_index));
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
                    else if (_args[0] == "print_time_ruler")
                    {
                        if (_args.size != 2)
                        {
                            write_oneline_response(command_id, @"Bad arguments number.", 1);
                            continue;
                        }
                        string on_off = _args[1].up();
                        if (on_off == "ON" && h_time_ruler == null)
                        {
                            // start a tasklet to keep time on console output.
                            h_time_ruler = tasklet.spawn(new KeepTimeTasklet());
                            write_empty_response(command_id);
                        }
                        else if (on_off == "OFF" && h_time_ruler != null)
                        {
                            // kill the tasklet
                            h_time_ruler.kill();
                            h_time_ruler = null;
                            write_empty_response(command_id);
                        }
                        else write_oneline_response(command_id, @"Bad argument $(on_off).", 1);
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
        int nodes_inside_0 = identity_data.main_id ? 1 : 0;
        string l0 = @"local_identity #$(index):";
        l0 += @" address $(my_naddr_str), elderships $(my_elderships_str),";
        string network_namespace_str = identity_data.network_namespace;
        if (network_namespace_str == "") network_namespace_str = "default";
        l0 += @" namespace $(network_namespace_str),";
        string l1 = @"                   Level 0: Fingerprint $(my_fp0). Nodes inside #$(nodes_inside_0).";
        ret.add(l0);
        ret.add(l1);
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(identity_data.nodeid, "qspn");
        for (int i = 1; i <= levels; i++)
        {
            string fp_i_s = "<BootstrapInProgress>";
            string nodes_inside_i_s = "<BootstrapInProgress>";
            try {
                Fingerprint fp_i = (Fingerprint)qspn_mgr.get_fingerprint(i);
                fp_i_s = @"$(fp_i.id)";
                int nodes_inside_i = qspn_mgr.get_nodes_inside(i);
                nodes_inside_i_s = @"$(nodes_inside_i)";
            } catch (QspnBootstrapInProgressError e) {}
            string l2 = @"                   Level $(i): Fingerprint $(fp_i_s). Nodes inside #$(nodes_inside_i_s).";
            ret.add(l2);
        }
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
        foreach (IdentityData identity_data in local_identities.values)
         foreach (IdentityArc ia in identity_data.identity_arcs.values)
         if (ia.arc == arc.idmgmt_arc && ia.qspn_arc != null)
        {
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(identity_data.nodeid, "qspn");
            qspn_mgr.arc_is_changed(ia.qspn_arc);
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

    Gee.List<string> show_identity_arcs(int local_identity_index)
    {
        IdentityData identity_data = local_identities[local_identity_index];
        ArrayList<string> ret = new ArrayList<string>();
        foreach (int i in identity_data.identity_arcs.keys)
        {
            ret.add_all(print_identity_arc(local_identity_index, i));
        }
        return ret;
    }

    Gee.List<string> print_identity_arc(int local_identity_index, int identity_arc_index)
    {
        IdentityData identity_data = local_identities[local_identity_index];
        IdentityArc ia = identity_data.identity_arcs[identity_arc_index];
        ArrayList<string> ret = new ArrayList<string>();
        ret.add(@"identity_arc #$(identity_arc_index) of local_identity #$(local_identity_index):");
        ret.add(@"    to $(ia.peer_mac)");
        if (ia.qspn_arc == null)
        {
            ret.add(@"    no qspn_arc");
        }
        else
        {
            if (ia.tablename == null)
            {
                ret.add(@"    no tablename");
            }
            else
            {
                string rule_added = "no rule.";
                if (ia.rule_added == true) rule_added = "rule added.";
                ret.add(@"    tablename $(ia.tablename), tid $(ia.tid), $(rule_added)");
            }
        }
        // TODO ....
        return ret;
    }

    void update_all_routes(int local_identity_index)
    {
        IdentityData identity_data = local_identities[local_identity_index];
        per_identity_update_all_routes(identity_data);
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
        QspnManager old_id_qspn_mgr = (QspnManager)(identity_mgr.get_identity_module(old_id, "qspn"));
        NodeID new_id = identity_mgr.add_identity(op_id, old_id);
        // This produced some signal `identity_arc_added`: hence some IdentityArc instances have been created
        //  and stored in `new_identity_data.my_identityarcs`.
        IdentityData new_identity_data = find_or_create_local_identity(new_id);
        new_identity_data.copy_of_identity = old_identity_data;
        new_identity_data.connectivity_from_level = old_identity_data.connectivity_from_level;
        new_identity_data.connectivity_to_level = old_identity_data.connectivity_to_level;

        old_identity_data.connectivity_from_level = op.guest_gnode_level + 1;
        old_identity_data.connectivity_to_level = levels - 1;

        HashMap<IdentityArc, IdentityArc> old_to_new_id_arc = new HashMap<IdentityArc, IdentityArc>();
        foreach (IdentityArc w0 in old_identity_data.identity_arcs.values)
        {
            bool old_identity_arc_changed_peer_mac = (w0.prev_peer_mac != null);
            // find appropriate w1
            foreach (IdentityArc w1 in new_identity_data.identity_arcs.values)
            {
                if (w1.arc != w0.arc) continue;
                if (old_identity_arc_changed_peer_mac)
                {
                    if (w1.peer_mac != w0.prev_peer_mac) continue;
                }
                else
                {
                    if (w1.peer_mac != w0.peer_mac) continue;
                }
                old_to_new_id_arc[w0] = w1;
                break;
            }
        }

        string old_ns = new_identity_data.network_namespace;
        string new_ns = old_identity_data.network_namespace;

        {
            // Change address of connectivity identity.
            int ch_level = op.guest_gnode_level;
            int ch_pos = op.connectivity_pos;
            int ch_eldership = op.connectivity_pos_eldership;
            int64 fp_id = old_identity_data.my_fp.id;

            QspnManager.ChangeNaddrDelegate update_naddr = (_a) => {
                Naddr a = (Naddr)_a;
                ArrayList<int> _naddr_temp = new ArrayList<int>();
                _naddr_temp.add_all(a.pos);
                _naddr_temp[ch_level] = ch_pos;
                return new Naddr(_naddr_temp.to_array(), _gsizes.to_array());
            };

            ArrayList<int> _elderships_temp = new ArrayList<int>();
            _elderships_temp.add_all(old_identity_data.my_fp.elderships);
            _elderships_temp[ch_level] = ch_eldership;

            old_identity_data.my_naddr = (Naddr)update_naddr(old_identity_data.my_naddr);
            old_identity_data.my_fp = new Fingerprint(_elderships_temp.to_array(), fp_id);
            old_id_qspn_mgr.make_connectivity(op.guest_gnode_level+1, levels, update_naddr, old_identity_data.my_fp);
        }

        HashMap<int,HashMap<int,DestinationIPSet>> old_destination_ip_set;
        old_destination_ip_set = copy_destination_ip_set(old_identity_data.destination_ip_set);
        compute_destination_ip_set(old_identity_data.destination_ip_set, old_identity_data.my_naddr);

        ArrayList<string> prefix_cmd_old_ns = new ArrayList<string>();
        if (old_ns != "") prefix_cmd_old_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(old_ns)"});
        ArrayList<string> prefix_cmd_new_ns = new ArrayList<string>.wrap({
            @"ip", @"netns", @"exec", @"$(new_ns)"});

        // Move routes of old identity into new network namespace
        // Search qspn-arc of old identity. For them we have tables in old network namespace
        //  which are to be copied in new network namespace.
        int bid6 = cm.begin_block();
        foreach (IdentityArc ia in old_identity_data.identity_arcs.values) if (ia.qspn_arc != null)
        {
            string ipaddr;
            ArrayList<string> cmd;
            cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_new_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                @"-m", @"mac", @"--mac-source", @"$(ia.peer_mac)",
                @"-j", @"MARK", @"--set-mark", @"$(ia.tid)"});
            cm.single_command_in_block(bid6, cmd);
            // Add to the table the new destination IP set of old identity. Initially as unreachable.
            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (old_identity_data.destination_ip_set[i][j].global != "")
                {
                    ipaddr = old_identity_data.destination_ip_set[i][j].global;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_new_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid6, cmd);
                    ipaddr = old_identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_new_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid6, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (old_identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        ipaddr = old_identity_data.destination_ip_set[i][j].intern[k];
                        cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_new_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                        cm.single_command_in_block(bid6, cmd);
                    }
                }
            }
            ia.rule_added = false;
        }
        // Then update routes for those neighbor we already know
        ArrayList<LookupTable> tables = new ArrayList<LookupTable>();
        foreach (NeighborData neighbor in all_neighbors(old_identity_data, true))
            tables.add(new LookupTable.forwarding(neighbor.tablename, neighbor));
        per_identity_foreach_lookuptable_update_all_best_paths(old_identity_data, tables, bid6);
        check_first_etp_from_arcs(old_identity_data, bid6);
        cm.end_block(bid6);

        // Remove old destination IPs from all tables in old network namespace
        int bid = cm.begin_block();
        ArrayList<string> tablenames = new ArrayList<string>();
        if (old_ns == "") tablenames.add("ntk");
        // Add the table that was in old namespace for each qspn-arc of old identity
        foreach (IdentityArc ia in old_identity_data.identity_arcs.values) if (ia.qspn_arc != null)
        {
            tablenames.add(ia.prev_tablename);
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
                    @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid, cmd);
                ipaddr = old_destination_ip_set[i][j].anonymous;
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid, cmd);
            }
            for (int k = levels-1; k >= i+1 && k > op.guest_gnode_level; k--)
            {
                if (old_destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = old_destination_ip_set[i][j].intern[k];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
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
                    @"iptables", @"-t", @"nat", @"-D", @"POSTROUTING", @"-d", @"$(anonymousrange)",
                    @"-j", @"SNAT", @"--to", @"$(old_identity_data.local_ip_set.global)"}));
            }

            // remove local addresses (global, anon, intern)
            if (old_identity_data.local_ip_set.global != "")
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.global)/32", @"dev", @"$(dev)"}));
            if (old_identity_data.local_ip_set.anonymous != "" && accept_anonymous_requests)
                foreach (string dev in real_nics)
                cm.single_command(new ArrayList<string>.wrap({
                    @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.anonymous)/32", @"dev", @"$(dev)"}));
            for (int i = levels-1; i > op.guest_gnode_level; i--)
            {
                if (old_identity_data.local_ip_set.intern[i] != "")
                    foreach (string dev in real_nics)
                    cm.single_command(new ArrayList<string>.wrap({
                        @"ip", @"address", @"del", @"$(old_identity_data.local_ip_set.intern[i])/32", @"dev", @"$(dev)"}));
            }
        }

        // Add routes of new identity into old network namespace

        // Prepare Netsukuku address
        // new address = op.host_gnode_address + op.in_host_pos1
        //             + old_identity_data.my_naddr.pos.slice(0, op.host_gnode_level-1)
        ArrayList<int> _naddr_new = new ArrayList<int>();
        foreach (string s_piece in op.host_gnode_address.split(".")) _naddr_new.insert(0, int.parse(s_piece));
        _naddr_new.insert(0, op.in_host_pos1);
        _naddr_new.insert_all(0, old_identity_data.my_naddr.pos.slice(0, op.host_gnode_level-1));
        new_identity_data.my_naddr = new Naddr(_naddr_new.to_array(), _gsizes.to_array());
        // Prepare fingerprint
        // new elderships = op.host_gnode_elderships + op.in_host_pos1_eldership
        //                + 0 * (op.host_gnode_level - 1 - op.guest_gnode_level)
        //                + old_identity_data.my_fp.elderships.slice(0, op.guest_gnode_level)
        ArrayList<int> _elderships = new ArrayList<int>();
        foreach (string s_piece in op.host_gnode_elderships.split(".")) _elderships.insert(0, int.parse(s_piece));
        _elderships.insert(0, op.in_host_pos1_eldership);
        for (int jj = 0; jj < op.host_gnode_level - 1 - op.guest_gnode_level; jj++) _elderships.insert(0, 0);
        _elderships.insert_all(0, old_identity_data.my_fp.elderships.slice(0, op.guest_gnode_level));
        new_identity_data.my_fp = new Fingerprint(_elderships.to_array(), old_identity_data.my_fp.id);

        // Compute local IPs. The valid intern IP are already set in old network namespace.
        compute_local_ip_set(new_identity_data.local_ip_set, new_identity_data.my_naddr);
        // Compute destination IPs. Then, we add the routes in old network namespace.
        compute_destination_ip_set(new_identity_data.destination_ip_set, new_identity_data.my_naddr);

        // Add new destination IPs into all tables in old network namespace
        int bid2 = cm.begin_block();
        tablenames = new ArrayList<string>();
        if (old_ns == "") tablenames.add("ntk");
        // Add a table for each qspn-arc of old identity that will be also in new identity
        foreach (IdentityArc ia in old_identity_data.identity_arcs.values) if (ia.qspn_arc != null)
        {
            bool old_identity_arc_changed_peer_mac = (ia.prev_peer_mac != null);
            // If old identity's identity_arc has changed its peer_mac, then the previous mac will be
            //  used by new identity.
            if (old_identity_arc_changed_peer_mac)
            {
                tablenames.add(ia.prev_tablename);
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
                    @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid2, cmd);
                ipaddr = new_identity_data.destination_ip_set[i][j].anonymous;
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid2, cmd);
            }
            for (int k = levels-1; k >= i+1 && k > op.guest_gnode_level; k--)
            {
                if (new_identity_data.destination_ip_set[i][j].intern[k] != "")
                {
                    string ipaddr = new_identity_data.destination_ip_set[i][j].intern[k];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid2, cmd);
                }
            }
        }
        cm.end_block(bid2);

        // New qspn manager

        // Prepare internal arcs
        ArrayList<IQspnArc> internal_arc_set = new ArrayList<IQspnArc>();
        ArrayList<IQspnNaddr> internal_arc_peer_naddr_set = new ArrayList<IQspnNaddr>();
        foreach (IdentityArc w0 in old_identity_data.identity_arcs.values)
        {
            bool old_identity_arc_is_internal = (w0.prev_peer_mac != null);
            if (old_identity_arc_is_internal)
            {
                // It is an internal arc
                IdentityArc w1 = old_to_new_id_arc[w0]; // w1 is already in new_identity_data.my_identityarcs
                NodeID destid = w1.id_arc.get_peer_nodeid();
                NodeID sourceid = w1.id; // == new_id
                IdmgmtArc __arc = (IdmgmtArc)w1.arc;
                Arc _arc = __arc.arc;
                w1.qspn_arc = new QspnArc(_arc, sourceid, destid, w1, w1.peer_mac);
                tn.get_table(null, w1.peer_mac, out w1.tid, out w1.tablename);
                w1.rule_added = w0.prev_rule_added;

                assert(w0.qspn_arc != null);
                IQspnNaddr? _w0_peer_naddr = old_id_qspn_mgr.get_naddr_for_arc(w0.qspn_arc);
                assert(_w0_peer_naddr != null);
                Naddr w0_peer_naddr = (Naddr)_w0_peer_naddr;
                // w1_peer_naddr = new_identity_data.my_naddr.pos.slice(op.host_gnode_level-1, levels)
                //             + w0_peer_naddr.pos.slice(0, op.host_gnode_level-1)
                ArrayList<int> _w1_peer_naddr = new ArrayList<int>();
                _w1_peer_naddr.add_all(w0_peer_naddr.pos.slice(0, op.host_gnode_level-1));
                _w1_peer_naddr.add_all(new_identity_data.my_naddr.pos.slice(op.host_gnode_level-1, levels));
                Naddr w1_peer_naddr = new Naddr(_w1_peer_naddr.to_array(), _gsizes.to_array());

                // Now add: the 2 ArrayList should have same size at the end.
                internal_arc_set.add(w1.qspn_arc);
                internal_arc_peer_naddr_set.add(w1_peer_naddr);
            }
        }
        // Prepare external arcs
        ArrayList<IQspnArc> external_arc_set = new ArrayList<IQspnArc>();
        foreach (int w0_index in op.id_arc_index_list)
        {
            IdentityArc w0 = old_identity_data.identity_arcs[w0_index];
            IdentityArc w1 = old_to_new_id_arc[w0]; // w1 is already in new_identity_data.my_identityarcs
            NodeID destid = w1.id_arc.get_peer_nodeid();
            NodeID sourceid = w1.id; // == new_id
            IdmgmtArc __arc = (IdmgmtArc)w1.arc;
            Arc _arc = __arc.arc;
            w1.qspn_arc = new QspnArc(_arc, sourceid, destid, w1, w1.peer_mac);
            tn.get_table(null, w1.peer_mac, out w1.tid, out w1.tablename);
            w1.rule_added = false;

            external_arc_set.add(w1.qspn_arc);
        }
        // Prepare mapping old-arcs to new-arcs (duplicates)
        QspnManager.PreviousArcToNewArcDelegate old_arc_to_new_arc = (/*IQspnArc*/ old_arc) => {
            // return IQspnArc or null.
            foreach (IdentityArc old_identity_arc in old_to_new_id_arc.keys)
            {
                if (old_identity_arc.qspn_arc == old_arc)
                    return old_to_new_id_arc[old_identity_arc].qspn_arc;
            }
            return null;
        };
        // Create new qspn manager
        QspnManager qspn_mgr = new QspnManager.enter_net(
            new_identity_data.my_naddr,
            internal_arc_set,
            internal_arc_peer_naddr_set,
            external_arc_set,
            old_arc_to_new_arc,
            new_identity_data.my_fp,
            new QspnStubFactory(new_identity_data),
            /*hooking_gnode_level*/ op.guest_gnode_level,
            /*into_gnode_level*/ op.host_gnode_level,
            /*previous_identity*/ old_id_qspn_mgr);
        // soon after creation, connect to signals.
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

        identity_mgr.set_identity_module(new_id, "qspn", qspn_mgr);
        new_identity_data.addr_man = new AddressManagerForIdentity(qspn_mgr);

        // Add new destination IPs into new forwarding-tables in old network namespace
        int bid3 = cm.begin_block();
        foreach (IdentityArc ia in new_identity_data.identity_arcs.values)
         if (ia.qspn_arc != null)
         if (ia.qspn_arc in external_arc_set)
        {
            ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                @"-m", @"mac", @"--mac-source", @"$(ia.peer_mac)",
                @"-j", @"MARK", @"--set-mark", @"$(ia.tid)"});
            cm.single_command_in_block(bid3, cmd);

            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (new_identity_data.destination_ip_set[i][j].global != "")
                {
                    string ipaddr = new_identity_data.destination_ip_set[i][j].global;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid3, cmd);
                    ipaddr = new_identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid3, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (new_identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        string ipaddr = new_identity_data.destination_ip_set[i][j].intern[k];
                        cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                        cm.single_command_in_block(bid3, cmd);
                    }
                }
            }
        }
        cm.end_block(bid3);

        // Netsukuku Address of new identity will be changing.
        if (op.prev_op_id == null)
        {
            // Immediately change address of new identity.
            {
                // Change address of new identity.
                int ch_level = op.host_gnode_level-1;
                int ch_pos = op.in_host_pos2;
                int ch_eldership = op.in_host_pos2_eldership;
                int64 fp_id = new_identity_data.my_fp.id;

                QspnManager.ChangeNaddrDelegate update_naddr = (_a) => {
                    Naddr a = (Naddr)_a;
                    ArrayList<int> _naddr_temp = new ArrayList<int>();
                    _naddr_temp.add_all(a.pos);
                    _naddr_temp[ch_level] = ch_pos;
                    return new Naddr(_naddr_temp.to_array(), _gsizes.to_array());
                };

                ArrayList<int> _elderships_temp = new ArrayList<int>();
                _elderships_temp.add_all(new_identity_data.my_fp.elderships);
                _elderships_temp[ch_level] = ch_eldership;

                new_identity_data.my_naddr = (Naddr)update_naddr(new_identity_data.my_naddr);
                new_identity_data.my_fp = new Fingerprint(_elderships_temp.to_array(), fp_id);
                qspn_mgr.make_real(update_naddr, new_identity_data.my_fp);
            }

            int bid4 = cm.begin_block();
            tables = new ArrayList<LookupTable>();
            if (old_ns == "") tables.add(new LookupTable.egress("ntk"));
            // Add a table for each qspn-arc of new identity
            foreach (IdentityArc ia in new_identity_data.identity_arcs.values) if (ia.qspn_arc != null)
                tables.add(new LookupTable.forwarding(ia.tablename, get_neighbor(new_identity_data, ia)));
            HashMap<int,HashMap<int,DestinationIPSet>> prev_new_identity_destination_ip_set;
            prev_new_identity_destination_ip_set = copy_destination_ip_set(new_identity_data.destination_ip_set);
            compute_destination_ip_set(new_identity_data.destination_ip_set, new_identity_data.my_naddr);
            foreach (LookupTable table in tables)
             for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                string tablename = table.tablename;
                bool must_update = false;
                if (new_identity_data.destination_ip_set[i][j].global != "" &&
                    prev_new_identity_destination_ip_set[i][j].global == "")
                {
                    must_update = true;
                    // add route for i,j.global for $tablename
                    string ipaddr = new_identity_data.destination_ip_set[i][j].global;
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid4, cmd);
                    // add route for i,j.anonymous for $tablename
                    ipaddr = new_identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid4, cmd);
                }
                else if (new_identity_data.destination_ip_set[i][j].global == "" &&
                    prev_new_identity_destination_ip_set[i][j].global != "")
                {
                    string ipaddr = prev_new_identity_destination_ip_set[i][j].global;
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid4, cmd);
                    ipaddr = prev_new_identity_destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid4, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (new_identity_data.destination_ip_set[i][j].intern[k] != "" &&
                        prev_new_identity_destination_ip_set[i][j].intern[k] == "")
                    {
                        must_update = true;
                        // add route for i,j.intern[k] for $tablename
                        string ipaddr = new_identity_data.destination_ip_set[i][j].intern[k];
                        ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(tablename)"});
                        cm.single_command_in_block(bid4, cmd);
                    }
                    else if (new_identity_data.destination_ip_set[i][j].intern[k] == "" &&
                        prev_new_identity_destination_ip_set[i][j].intern[k] != "")
                    {
                        string ipaddr = prev_new_identity_destination_ip_set[i][j].intern[k];
                        ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_old_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"del", @"$(ipaddr)", @"table", @"$(tablename)"});
                        cm.single_command_in_block(bid4, cmd);
                    }
                }
                if (must_update)
                {
                    if (table.pkt_egress || table.pkt_from.h != null)
                    {
                        // update route for whole (i,j) for $table
                        BestRouteToDest? best = per_identity_per_lookuptable_find_best_path_to_h(
                                                new_identity_data, table, new HCoord(i, j));
                        per_identity_per_lookuptable_update_best_path_to_h(
                            new_identity_data,
                            table,
                            best,
                            new HCoord(i, j),
                            bid4);
                    }
                }
            }
            cm.end_block(bid4);

            if (old_ns == "")
            {
                // Add, when needed, local IPs and SNAT rule.

                LocalIPSet prev_new_identity_local_ip_set;
                prev_new_identity_local_ip_set = copy_local_ip_set(new_identity_data.local_ip_set);
                compute_local_ip_set(new_identity_data.local_ip_set, new_identity_data.my_naddr);

                for (int i = 1; i < levels; i++)
                {
                    if (new_identity_data.local_ip_set.intern[i] != "" &&
                        prev_new_identity_local_ip_set.intern[i] == "")
                    {
                        foreach (string dev in real_nics)
                            cm.single_command(new ArrayList<string>.wrap({
                                @"ip", @"address", @"add", @"$(new_identity_data.local_ip_set.intern[i])", @"dev", @"$(dev)"}));
                    }
                }
                if (new_identity_data.local_ip_set.global != "" &&
                    prev_new_identity_local_ip_set.global == "")
                {
                    foreach (string dev in real_nics)
                        cm.single_command(new ArrayList<string>.wrap({
                            @"ip", @"address", @"add", @"$(new_identity_data.local_ip_set.global)", @"dev", @"$(dev)"}));
                    if (! no_anonymize)
                    {
                        string anonymousrange = ip_anonymizing_gnode(new_identity_data.my_naddr.pos, levels);
                        cm.single_command(new ArrayList<string>.wrap({
                            @"iptables", @"-t", @"nat", @"-A", @"POSTROUTING", @"-d", @"$(anonymousrange)",
                            @"-j", @"SNAT", @"--to", @"$(new_identity_data.local_ip_set.global)"}));
                    }
                    if (accept_anonymous_requests)
                    {
                        foreach (string dev in real_nics)
                            cm.single_command(new ArrayList<string>.wrap({
                                @"ip", @"address", @"add", @"$(new_identity_data.local_ip_set.anonymous)", @"dev", @"$(dev)"}));
                    }
                }

                // update only table ntk because of updated "src"
                int bid5 = cm.begin_block();
                tables = new ArrayList<LookupTable>();
                tables.add(new LookupTable.egress("ntk"));
                per_identity_foreach_lookuptable_update_all_best_paths(new_identity_data, tables, bid5);
                cm.end_block(bid5);
            }
        }
        else
        {
            // TODO we wait for command `enter_net_phase_2`
        }

        // Finally, the peer_mac and peer_linklocal of the identity-arcs of old identity
        // can be updated (if they need to).
        foreach (IdentityArc ia in old_identity_data.identity_arcs.values) if (ia.qspn_arc != null)
        {
            ia.peer_mac = ia.id_arc.get_peer_mac();
            ia.peer_linklocal = ia.id_arc.get_peer_linklocal();
        }

        // Remove old identity
        identity_mgr.remove_identity(old_identity_data.nodeid);
        old_id_qspn_mgr.stop_operations();
        remove_local_identity(old_identity_data.nodeid);
        foreach (IdentityArc ia in old_identity_data.identity_arcs.values) if (ia.tid != null)
        {
            print(@"check arc to $(ia.peer_mac): still used?\n");
            bool still_used = false;
            foreach (IdentityData id1 in local_identities.values)
            {
                foreach (IdentityArc idarc1 in id1.identity_arcs.values)
                {
                    if (idarc1.tid == ia.tid)
                    {
                        still_used = true;
                        break;
                    }
                }
                if (still_used) break;
            }
            if (! still_used) {print("no.\n"); tn.release_table(null, ia.peer_mac);}
            else print("yes.\n");
        }
        print("done.\n");
    }

    void add_qspn_arc(int local_identity_index, string my_dev, string peer_mac)
    {
        IdentityData identity_data = local_identities[local_identity_index];
        NodeID sourceid = identity_data.nodeid;
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(sourceid, "qspn");

        foreach (IdentityArc ia in identity_data.identity_arcs.values)
         if (((IdmgmtArc)ia.arc).arc.neighborhood_arc.nic.dev.up() == my_dev.up())
         if (ia.peer_mac == peer_mac)
        {
            NodeID destid = ia.id_arc.get_peer_nodeid();
            IdmgmtArc _arc = (IdmgmtArc)ia.arc;
            Arc arc = _arc.arc;
            ia.qspn_arc = new QspnArc(arc, sourceid, destid, ia, ia.peer_mac);
            tn.get_table(null, ia.peer_mac, out ia.tid, out ia.tablename);
            ia.rule_added = false;
            qspn_mgr.arc_add(ia.qspn_arc);

            // Add new forwarding-table
            int bid = cm.begin_block();
            string ns = identity_data.network_namespace;
            ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
            if (ns != "") prefix_cmd_ns.add_all_array({
                @"ip", @"netns", @"exec", @"$(ns)"});
            ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                @"-m", @"mac", @"--mac-source", @"$(peer_mac)",
                @"-j", @"MARK", @"--set-mark", @"$(ia.tid)"});
            cm.single_command_in_block(bid, cmd);

            for (int i = levels-1; i >= subnetlevel; i--)
             for (int j = 0; j < _gsizes[i]; j++)
            {
                if (identity_data.destination_ip_set[i][j].global != "")
                {
                    string ipaddr = identity_data.destination_ip_set[i][j].global;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid, cmd);
                    ipaddr = identity_data.destination_ip_set[i][j].anonymous;
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid, cmd);
                }
                for (int k = levels-1; k >= i+1; k--)
                {
                    if (identity_data.destination_ip_set[i][j].intern[k] != "")
                    {
                        string ipaddr = identity_data.destination_ip_set[i][j].intern[k];
                        cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                        cm.single_command_in_block(bid, cmd);
                    }
                }
            }
            cm.end_block(bid);
        }
    }

    Gee.List<string> show_qspn_data(int local_identity_index)
    {
        ArrayList<string> ret = new ArrayList<string>();
        IdentityData identity_data = local_identities[local_identity_index];
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(identity_data.nodeid, "qspn");
        Gee.List<QspnArc> arcs = (Gee.List<QspnArc>)qspn_mgr.current_arcs();
        foreach (QspnArc arc in arcs)
        {
            IQspnNaddr? naddr = qspn_mgr.get_naddr_for_arc(arc);
            string s_naddr = "null";
            if (naddr != null) s_naddr = naddr_repr((Naddr)naddr);
            ret.add(@"QspnArc to $(s_naddr), cost $(arc.arc.cost)");
        }
        for (int lvl = 0; lvl < levels; lvl++)
        {
            try {
                Gee.List<HCoord> lst_h = qspn_mgr.get_known_destinations(lvl);
                foreach (HCoord h in lst_h)
                {
                    // public Gee.List<Netsukuku.Qspn.IQspnNodePath> get_paths_to (Netsukuku.HCoord d)
                    Gee.List<IQspnNodePath> lst_paths = qspn_mgr.get_paths_to(h);
                    foreach (IQspnNodePath path in lst_paths)
                    {
                        ret.add(@"Path to ($(h.lvl), $(h.pos)) with $(path.i_qspn_get_hops().size) hops");
                    }
                }
            } catch (QspnBootstrapInProgressError e) {
            }
        }
        return ret;
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

    class KeepTimeTasklet : Object, ITaskletSpawnable
    {
        public void * func()
        {
            int prev_second = -1;
            int prev_quarter = -1;
            while (true)
            {
                print(".\n");
                DateTime now = new DateTime.now_local();
                int now_second = now.get_second();
                int now_quarter = now.get_microsecond() / 250000;
                if (now_second != prev_second || now_quarter != prev_quarter)
                {
                    prev_second = now_second;
                    prev_quarter = now_quarter;
                    print(@". $(now.format("%FT%H:%M:%S"))+$(now_quarter*25)\n");
                }
                tasklet.ms_wait(3);
            }
        }
    }
}
