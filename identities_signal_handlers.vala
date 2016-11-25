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

    void identities_identity_arc_changed(IIdmgmtArc arc, NodeID id, IIdmgmtIdentityArc id_arc)
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
            update_best_paths_forall_destinations_per_identity(identity_data);
            print(@"Debug: IdentityData #$(identity_data.local_identity_index): done update_all_destinations for identity_arc_changed.\n");
        }
    }

    void identities_identity_arc_removing(IIdmgmtArc arc, NodeID id, NodeID peer_nodeid)
    {
        // Retrieve my identity.
        IdentityData identity_data = find_or_create_local_identity(id);
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
