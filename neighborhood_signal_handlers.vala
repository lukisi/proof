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
    void neighborhood_nic_address_set(string my_dev, string my_addr)
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

    void neighborhood_arc_added(INeighborhoodArc arc)
    {
        string k = key_for_physical_arc(arc.nic.mac, arc.neighbour_mac);
        assert(! (k in neighborhood_arcs.keys));
        neighborhood_arcs[k] = arc;
        print(@"neighborhood_arc '$(k)': peer_linklocal $(arc.neighbour_nic_addr), cost $(arc.cost)us\n");
    }

    void neighborhood_arc_changed(INeighborhoodArc arc)
    {
        //print(@"arc_changed (no effect) for $(arc.neighbour_nic_addr)\n");
    }

    void neighborhood_arc_removing(INeighborhoodArc arc, bool is_still_usable)
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

    void neighborhood_arc_removed(INeighborhoodArc arc)
    {
        string k = key_for_physical_arc(arc.nic.mac, arc.neighbour_mac);
        print(@"Neighborhood module: neighborhood_arc `$(k)` has been removed.\n");
        neighborhood_arcs.unset(k);
    }

    void neighborhood_nic_address_unset(string my_dev, string my_addr)
    {
    }
}
