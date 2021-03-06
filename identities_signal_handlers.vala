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

    void identities_identity_arc_added(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
    {
        print("An identity-arc has been added.\n");
        IdentityData identity_data = find_or_create_local_identity(id);
        IdentityArc ia = new IdentityArc(identity_data, arc, id_arc);
        foreach (string s in print_identity_arc(identity_data.local_identity_index, ia.identity_arc_index)) print(s + "\n");
    }

    void identities_identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc, bool only_neighbour_migrated)
    {
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
        // Retrieve peer_nodeid
        NodeID peer_nodeid = id_arc.get_peer_nodeid();
        // Retrieve IdentityArc
        IdentityArc ia = find_identity_arc(identity_data, arc, peer_nodeid);
        print("An identity-arc has been changed.\n");

        ia.prev_peer_mac = ia.peer_mac;
        ia.prev_peer_linklocal = ia.peer_linklocal;
        ia.prev_tablename = ia.tablename;
        ia.prev_tid = ia.tid;
        ia.prev_rule_added = ia.rule_added;
        ia.peer_mac = ia.id_arc.get_peer_mac();
        ia.peer_linklocal = ia.id_arc.get_peer_linklocal();
        ia.tablename = null;
        ia.tid = null;
        ia.rule_added = null;
        if (ia.qspn_arc != null)
        {
            tn.get_table(null, ia.peer_mac, out ia.tid, out ia.tablename);
            ia.rule_added = false;
        }

        foreach (string s in print_identity_arc(identity_data.local_identity_index, ia.identity_arc_index)) print(s + "\n");

        // This should be the same instance.
        assert(ia.id_arc == id_arc);

        // This signal might happen when the module Identities of this system is doing `add_identity` on
        //  this very identity (identity_data).
        //  In this case the program does some further operations on its own (see user_commands.vala).
        //  But this might also happen when only our neighbour is doing `add_identity`.
        if (only_neighbour_migrated)
        {
            print("   Only neighbour migrated.\n");
            //  In this case we must do some work if we have a qspn_arc on this identity_arc.
            if (ia.qspn_arc != null)
            {
                print("   Neighbour is in my network.\n");
                string ns = identity_data.network_namespace;
                ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
                if (ns != "") prefix_cmd_ns.add_all_array({
                    @"ip", @"netns", @"exec", @"$(ns)"});
                ArrayList<string> cmd;
                string ipaddr;
                int bid = cm.begin_block();

                // remove old forwarding table
                if (ia.prev_rule_added == true)
                {
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"rule", @"del", @"fwmark", @"$(ia.prev_tid)",
                        @"table", @"$(ia.prev_tablename)"});
                    cm.single_command_in_block(bid, cmd);
                    ia.rule_added = false;
                }
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"flush", @"table", @"$(ia.prev_tablename)"});
                cm.single_command_in_block(bid, cmd);
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"iptables", @"-t", @"mangle", @"-D", @"PREROUTING", @"-m", @"mac",
                    @"--mac-source", @"$(ia.prev_peer_mac)", @"-j", @"MARK", @"--set-mark", @"$(ia.prev_tid)"});
                cm.single_command_in_block(bid, cmd);
                bool still_used = false;
                foreach (IdentityData id1 in local_identities.values)
                {
                    if (id1 != identity_data)
                    {
                        foreach (IdentityArc idarc1 in id1.identity_arcs.values)
                        {
                            if (idarc1.tid == ia.prev_tid)
                            {
                                still_used = true;
                                break;
                            }
                        }
                        if (still_used) break;
                    }
                }
                if (! still_used) tn.release_table(bid, ia.prev_peer_mac);

                // add new forwarding table
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"iptables", @"-t", @"mangle", @"-A", @"PREROUTING",
                    @"-m", @"mac", @"--mac-source", @"$(ia.peer_mac)",
                    @"-j", @"MARK", @"--set-mark", @"$(ia.tid)"});
                cm.single_command_in_block(bid, cmd);
                // Add to the table the new destination IP set of old identity. Initially as unreachable.
                for (int i = levels-1; i >= subnetlevel; i--)
                 for (int j = 0; j < _gsizes[i]; j++)
                {
                    if (identity_data.destination_ip_set[i][j].global != "")
                    {
                        ipaddr = identity_data.destination_ip_set[i][j].global;
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
                            ipaddr = identity_data.destination_ip_set[i][j].intern[k];
                            cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                            cmd.add_all_array({
                                @"ip", @"route", @"add", @"unreachable", @"$(ipaddr)", @"table", @"$(ia.tablename)"});
                            cm.single_command_in_block(bid, cmd);
                        }
                    }
                }
                cm.end_block(bid);

                // if neighbor was known
                if (ia.prev_rule_added == true)
                {
                    // update all routes in all tables and add rule.
                    per_identity_update_all_routes(identity_data);
                }
            }
            ia.prev_peer_mac = null;
            ia.prev_peer_linklocal = null;
            ia.prev_tablename = null;
            ia.prev_tid = null;
            ia.prev_rule_added = null;
        }
    }

    void identities_identity_arc_removing(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        print("An identity-arc is going to be removed.\n");
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
        // Retrieve identity-arc.
        IdentityArc ia = find_identity_arc(identity_data, arc, peer_nodeid);

        foreach (string s in print_identity_arc(identity_data.local_identity_index, ia.identity_arc_index)) print(s + "\n");

        string ns = identity_data.network_namespace;
        ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
        if (ns != "") prefix_cmd_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(ns)"});
        ArrayList<string> cmd;

        if (ia.qspn_arc != null)
        {
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");
            qspn_mgr.arc_remove(ia.qspn_arc);

            if (ia.rule_added == true)
            {
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"rule", @"del", @"fwmark", @"$(ia.tid)",
                    @"table", @"$(ia.tablename)"});
                cm.single_command(cmd);
                ia.rule_added = false;
            }
            cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
            cmd.add_all_array({
                @"ip", @"route", @"flush", @"table", @"$(ia.tablename)"});
            cm.single_command(cmd);
            cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
            cmd.add_all_array({
                @"iptables", @"-t", @"mangle", @"-D", @"PREROUTING", @"-m", @"mac",
                @"--mac-source", @"$(ia.peer_mac)", @"-j", @"MARK", @"--set-mark", @"$(ia.tid)"});
            cm.single_command(cmd);
            bool still_used = false;
            foreach (IdentityData id1 in local_identities.values)
            {
                if (id1 != identity_data)
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
            }
            if (! still_used) tn.release_table(null, ia.peer_mac);

            ia.qspn_arc = null;
            ia.tid = null;
            ia.tablename = null;
            ia.rule_added = null;
        }
    }

    void identities_identity_arc_removed(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        print("An identity-arc has been removed.\n");
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
        // Retrieve identity-arc.
        IdentityArc ia = find_identity_arc(identity_data, arc, peer_nodeid);

        foreach (string s in print_identity_arc(identity_data.local_identity_index, ia.identity_arc_index)) print(s + "\n");

        identity_data.identity_arcs.unset(ia.identity_arc_index);
    }

    void identities_arc_removed(IIdmgmtArc arc)
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
}
