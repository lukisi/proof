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
    void per_identity_qspn_arc_removed(IdentityData id, IQspnArc arc, string message, bool bad_link)
    {
        QspnArc _arc = (QspnArc)arc;
        warning(@"Qspn asks to remove arc: Identity #$(id.local_identity_index), arc to $(_arc.peer_mac)," +
                @" bad_link=$(bad_link), message=$(message)\n");
        if (bad_link)
        {
            // Remove arc from neighborhood, because it fails.
            neighborhood_mgr.remove_my_arc(_arc.arc.neighborhood_arc, false);
        }
        else
        {
            bool is_main_id_arc = false;
            // check if it is.
            if (id.main_id)
            {
                IdentityArc ia = _arc.ia;
                Arc node_arc = ((IdmgmtArc)ia.arc).arc;
                if (ia.peer_mac == node_arc.neighborhood_arc.neighbour_mac) is_main_id_arc = true;
            }
            if (is_main_id_arc)
            {
                neighborhood_mgr.remove_my_arc(_arc.arc.neighborhood_arc, false);
            }
            else
            {
                identity_mgr.remove_identity_arc(_arc.arc.idmgmt_arc, _arc.sourceid, _arc.destid, true);
            }
        }
        // Further actions are taken on signal identity_arc_removing of identity_mgr.
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
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            string _dest = @"($(h.lvl), $(h.pos))";
            print(@"destination_added: Identity #$(_id) ($(_naddr)), to $(_dest).\n");;
        }
        // something to do?
    }

    void per_identity_qspn_destination_removed(IdentityData id, HCoord h)
    {
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            string _dest = @"($(h.lvl), $(h.pos))";
            print(@"destination_removed: Identity #$(_id) ($(_naddr)), to $(_dest).\n");;
        }
        // something to do?
    }

    void per_identity_qspn_gnode_splitted(IdentityData id, IQspnArc a, HCoord d, IQspnFingerprint fp)
    {
        // TODO
        // we should do something of course
        warning("signal qspn_gnode_splitted: not implemented yet");
    }

    void per_identity_qspn_path_added(IdentityData id, IQspnNodePath p)
    {
        HCoord dest = p.i_qspn_get_hops().last().i_qspn_get_hcoord();
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            string _dest = @"($(dest.lvl), $(dest.pos))";
            print(@"path_added: Identity #$(_id) ($(_naddr)), to $(_dest).\n");;
        }
        int bid = cm.begin_block();
        per_identity_foreach_lookuptable_update_best_path_to_h(id, dest, bid);
        check_first_etp_from_arcs(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_path_changed(IdentityData id, IQspnNodePath p)
    {
        HCoord dest = p.i_qspn_get_hops().last().i_qspn_get_hcoord();
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            string _dest = @"($(dest.lvl), $(dest.pos))";
            print(@"path_changed: Identity #$(_id) ($(_naddr)), to $(_dest).\n");;
        }
        int bid = cm.begin_block();
        per_identity_foreach_lookuptable_update_best_path_to_h(id, dest, bid);
        check_first_etp_from_arcs(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_path_removed(IdentityData id, IQspnNodePath p)
    {
        HCoord dest = p.i_qspn_get_hops().last().i_qspn_get_hcoord();
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            string _dest = @"($(dest.lvl), $(dest.pos))";
            print(@"path_removed: Identity #$(_id) ($(_naddr)), to $(_dest).\n");;
        }
        int bid = cm.begin_block();
        per_identity_foreach_lookuptable_update_best_path_to_h(id, dest, bid);
        check_first_etp_from_arcs(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_presence_notified(IdentityData id)
    {
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            print(@"presence_notified: Identity #$(_id) ($(_naddr)).\n");;
        }
        if (id.copy_of_identity != null)
        {
            // Continue operations of connectivity: remove outer arcs and
            //  in a new tasklet keep an eye for when we can dismiss.
            do_connectivity(id.copy_of_identity);
        }
    }

    void per_identity_qspn_qspn_bootstrap_complete(IdentityData id)
    {
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            print(@"qspn_bootstrap_complete: Identity #$(_id) ($(_naddr)).\n");
            foreach (string s in print_local_identity(_id)) print(s + "\n");
        }
        // Update routes for table egress and tables forward of those neighbor we already know
        int bid = cm.begin_block();
        ArrayList<LookupTable> tables = new ArrayList<LookupTable>();
        if (id.network_namespace == "") tables.add(new LookupTable.egress("ntk"));
        foreach (NeighborData neighbor in all_neighbors(id, true))
            tables.add(new LookupTable.forwarding(neighbor.tablename, neighbor));
        per_identity_foreach_lookuptable_update_all_best_paths(id, tables, bid);
        check_first_etp_from_arcs(id, bid);
        cm.end_block(bid);
    }

    void per_identity_qspn_remove_identity(IdentityData id)
    {
        {
            int _id = id.local_identity_index;
            string _naddr = naddr_repr(id.my_naddr);
            print(@"remove_identity: Identity #$(_id) ($(_naddr)).\n");;
        }
        // The qspn manager wants to remove this connectivity identity because the connectivity is guaranteed.
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");
        qspn_mgr.destroy();
        identity_mgr.unset_identity_module(id.nodeid, "qspn");
        identity_mgr.remove_identity(id.nodeid);
        // remove identity and its id-arcs from memory data-structures
        id.identity_arcs.clear();
        local_identities.unset(id.local_identity_index);
    }
}
