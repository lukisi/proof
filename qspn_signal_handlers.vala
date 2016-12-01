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
    void per_identity_qspn_arc_removed(IdentityData id, IQspnArc arc, bool bad_link)
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

    void per_identity_qspn_changed_fp(IdentityData id, int l)
    {
        // TODO
    }

    void per_identity_qspn_changed_nodes_inside(IdentityData id, int l)
    {
        // TODO
    }

    void per_identity_qspn_destination_added(IdentityData id, HCoord h)
    {
        // something to do?
    }

    void per_identity_qspn_destination_removed(IdentityData id, HCoord h)
    {
        // something to do?
    }

    void per_identity_qspn_gnode_splitted(IdentityData id, IQspnArc a, HCoord d, IQspnFingerprint fp)
    {
        // TODO
        // we should do something of course
        error("not implemented yet");
    }

    void per_identity_qspn_path_added(IdentityData id, IQspnNodePath p)
    {
        int bid = cm.begin_block();
        update_best_paths_per_identity(id, p.i_qspn_get_hops().last().i_qspn_get_hcoord(), bid);
        update_rules(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_path_changed(IdentityData id, IQspnNodePath p)
    {
        int bid = cm.begin_block();
        update_best_paths_per_identity(id, p.i_qspn_get_hops().last().i_qspn_get_hcoord(), bid);
        update_rules(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_path_removed(IdentityData id, IQspnNodePath p)
    {
        int bid = cm.begin_block();
        update_best_paths_per_identity(id, p.i_qspn_get_hops().last().i_qspn_get_hcoord(), bid);
        update_rules(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_presence_notified(IdentityData id)
    {
        // TODO
    }

    void per_identity_qspn_qspn_bootstrap_complete(IdentityData id)
    {
        print(@"Debug: IdentityData #$(id.local_identity_index): call update_all_destinations for qspn_bootstrap_complete.\n");
        update_best_paths_forall_destinations_per_identity(id);
        print(@"Debug: IdentityData #$(id.local_identity_index): done update_all_destinations for qspn_bootstrap_complete.\n");
    }

    void update_best_paths_forall_destinations_per_identity(IdentityData id)
    {
        int bid = cm.begin_block();
        for (int lvl = levels - 1; lvl >= subnetlevel; lvl--)
         for (int pos = 0; pos < _gsizes[lvl]; pos++)
         if (id.my_naddr.pos[lvl] != pos)
        {
            update_best_paths_per_identity(id, new HCoord(lvl, pos), bid);
        }
        update_rules(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_remove_identity(IdentityData id)
    {
        // The qspn manager wants to remove this identity.
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");
        qspn_mgr.destroy();
        // We must remove identity from identity_manager. This will have IIdmgmtNetnsManager
        //  to remove pseudodevs and the network namespace. Beforehand, the NetworkStack
        //  instance has to be notified.
        id.network_stack.removing_namespace();
        identity_mgr.unset_identity_module(id.nodeid, "qspn");
        identity_mgr.remove_identity(id.nodeid);
        // remove identity and its id-arcs from memory data-structures
        local_identities.unset(id.local_identity_index);
        ArrayList<int> todel = new ArrayList<int>();
        foreach (int i in identityarcs.keys)
        {
            IdentityArc ia = identityarcs[i];
            NodeID node_id = ia.id;
            if (node_id.equals(id.nodeid)) todel.add(i);
        }
        foreach (int i in todel) identityarcs.unset(i);
    }
}
