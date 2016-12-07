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

    void identities_identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc, bool only_neighbour_migrated)
    {
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
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
            warning("I couldn't find it in memory.\n");
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
        // This might happen when the module Identities of this system is doing `add_identity` on
        //  this very identity (identity_data).
        //  In this case the program does some further operations on its own (see user_commands.vala).
        //  But this might also happen when only our neighbour is doing `add_identity`.
        if (only_neighbour_migrated)
        {
            //  In this case we must do some work if we have a qspn_arc on this identity_arc.
            if (ia.qspn_arc != null)
            {
                // TODO 
                error("not implemented yet");
            }
        }
    }

    void identities_identity_arc_removing(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
        string ns = identity_data.network_namespace;
        ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
        if (ns != "") prefix_cmd_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(ns)"});
        ArrayList<string> cmd;
        // Retrieve qspn_arc if still there.
        foreach (IdentityArc ia in identity_data.my_identityarcs)
            if (ia.arc == arc)
            if (ia.qspn_arc != null)
            if (ia.qspn_arc.destid.equals(peer_nodeid))
        {
            QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id, "qspn");
            qspn_mgr.arc_remove(ia.qspn_arc);

            string tablename;
            tn.get_table(null, ia.peer_mac, out ia.tid, out tablename);
            if (ia.rule_added)
            {
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"rule", @"del", @"fwmark", @"$(ia.tid)",
                    @"table", @"$(tablename)"});
                cm.single_command(cmd);
                ia.rule_added = false;
            }
            cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
            cmd.add_all_array({
                @"ip", @"route", @"flush", @"table", @"$(tablename)"});
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
                    foreach (IdentityArc idarc1 in id1.my_identityarcs)
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
            break;
        }
    }

    void identities_identity_arc_removed(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        print("An identity-arc has been removed.\n");
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
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
