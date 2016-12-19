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
    class LookupTable : Object
    {
        public string tablename;
        public bool pkt_egress;
        public NeighborData? pkt_from;

        public LookupTable.egress(string tablename)
        {
            this.tablename = tablename;
            pkt_egress = true;
            pkt_from = null;
        }

        public LookupTable.forwarding(string tablename, NeighborData pkt_from)
        {
            this.tablename = tablename;
            pkt_egress = false;
            this.pkt_from = pkt_from;
        }
    }

    class NeighborData : Object
    {
        public string mac;
        public string tablename;
        public HCoord? h;
    }

    NeighborData get_neighbor(IdentityData id, IdentityArc ia)
    {
        assert(ia.qspn_arc != null);

        // Compute neighbor.
        NeighborData ret = new NeighborData();
        ret.mac = ia.id_arc.get_peer_mac();
        ret.tablename = ia.tablename;

        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");
        IQspnNaddr? neighbour_naddr = qspn_mgr.get_naddr_for_arc(ia.qspn_arc);
        if (neighbour_naddr == null) ret.h = null;
        else ret.h = id.my_naddr.i_qspn_get_coord_by_address(neighbour_naddr);

        return ret;
    }

    Gee.List<NeighborData> all_neighbors(IdentityData id, bool only_known_peers=false)
    {
        // Compute list of neighbors.
        ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
        foreach (IdentityArc ia in id.identity_arcs.values) if (ia.qspn_arc != null)
        {
            NeighborData neighbor = get_neighbor(id, ia);
            if ((! only_known_peers) || neighbor.h != null)
                neighbors.add(neighbor);
        }
        return neighbors;
    }

    class BestRouteToDest : Object
    {
        public string gw;
        public string dev;
    }

    HashMap<string, BestRouteToDest>
    find_best_route_to_dest_foreach_table(
        Gee.List<IQspnNodePath> paths,
        Gee.List<NeighborData> neighbors)
    {
        HashMap<string, BestRouteToDest> best_route_foreach_table = new HashMap<string, BestRouteToDest>();
        foreach (IQspnNodePath path in paths)
        {
            QspnArc path_arc = (QspnArc)path.i_qspn_get_arc();
            string realnic = path_arc.arc.neighborhood_arc.nic.dev;
            string path_dev = identity_mgr.get_pseudodev(path_arc.sourceid, realnic);
            string gw = path_arc.ia.peer_linklocal;
            if (best_route_foreach_table.is_empty)
            {
                // absolute best.
                BestRouteToDest r = new BestRouteToDest();
                r.gw = gw;
                r.dev = path_dev;
                best_route_foreach_table["ntk"] = r;
            }
            bool completed = true;
            foreach (NeighborData neighbor in neighbors) if (neighbor.h != null)
            {
                // is it best without neighbor?
                // best_route_foreach_table contains tablename?
                if (best_route_foreach_table.has_key(neighbor.tablename)) continue;
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
                BestRouteToDest r = new BestRouteToDest();
                r.gw = gw;
                r.dev = path_dev;
                best_route_foreach_table[neighbor.tablename] = r;
            }
            if (completed) break;
        }
        return best_route_foreach_table;
    }

    BestRouteToDest? per_identity_per_lookuptable_find_best_path_to_h(
        IdentityData id,
        LookupTable table,
        HCoord h)
    {
        if (h.pos >= _gsizes[h.lvl]) return null; // ignore virtual destination.

        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
        if (! table.pkt_egress) neighbors.add(table.pkt_from);

        // Retrieve all routes towards `h`.
        Gee.List<IQspnNodePath> paths;
        try {
            paths = qspn_mgr.get_paths_to(h);
        } catch (QspnBootstrapInProgressError e) {
            paths = new ArrayList<IQspnNodePath>();
        }

        // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
        HashMap<string, BestRouteToDest> best_route_foreach_table =
            find_best_route_to_dest_foreach_table(paths, neighbors);

        BestRouteToDest? ret = null;
        if (best_route_foreach_table.has_key(table.tablename)) ret = best_route_foreach_table[table.tablename];
        return ret;
    }

    void per_identity_per_lookuptable_update_best_path_to_h(
        IdentityData id,
        LookupTable table,
        BestRouteToDest? best,
        HCoord h,
        int bid)
    {
        string tablename = table.tablename;
        string ns = id.network_namespace;
        ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
        if (ns != "") prefix_cmd_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(ns)"});
        DestinationIPSet h_ip_set = id.destination_ip_set[h.lvl][h.pos];
        if (table.pkt_egress) assert(ns == "");
        if (h_ip_set.global != "")
        {
            assert(h_ip_set.anonymous != "");
            if (best != null)
            {
                // set route global
                ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"$(h_ip_set.global)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                if (table.pkt_egress)
                 if (id.local_ip_set.global != "")
                    cmd.add_all_array({@"src", @"$(id.local_ip_set.global)"});
                cm.single_command_in_block(bid, cmd);
                // set route anonymous
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"$(h_ip_set.anonymous)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                if (table.pkt_egress)
                 if (id.local_ip_set.global != "")
                    cmd.add_all_array({@"src", @"$(id.local_ip_set.global)"});
                cm.single_command_in_block(bid, cmd);
            }
            else
            {
                // set unreachable global
                ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"unreachable", @"$(h_ip_set.global)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid, cmd);
                // set unreachable anonymous
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"unreachable", @"$(h_ip_set.anonymous)", @"table", @"$(tablename)"});
                cm.single_command_in_block(bid, cmd);
            }
        }
        for (int k = levels - 1; k > h.lvl; k--)
        {
            if (h_ip_set.intern[k] != "")
            {
                if ((! table.pkt_egress) && table.pkt_from.h.lvl >= k)
                {
                    // set blackhole intern
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"blackhole", @"$(h_ip_set.intern[k])", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid, cmd);
                }
                else if (best != null)
                {
                    // set route intern
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.intern[k])", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                    if (table.pkt_egress)
                     if (id.local_ip_set.intern[k] != "")
                        cmd.add_all_array({@"src", @"$(id.local_ip_set.intern[k])"});
                    cm.single_command_in_block(bid, cmd);
                }
                else
                {
                    // set unreachable intern
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"unreachable", @"$(h_ip_set.intern[k])", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid, cmd);
                }
            }
        }
    }

    void per_identity_per_lookuptable_update_all_best_paths(
        IdentityData id,
        LookupTable table,
        HashMap<HCoord, HashMap<string, BestRouteToDest>> all_best_routes,
        int bid)
    {
        for (int lvl = levels - 1; lvl >= subnetlevel; lvl--)
         for (int pos = 0; pos < _gsizes[lvl]; pos++)
         if (id.my_naddr.pos[lvl] != pos)
        {
            HCoord h = new HCoord(lvl, pos);
            BestRouteToDest? best = null;
            if (all_best_routes[h].has_key(table.tablename)) best = all_best_routes[h][table.tablename];
            per_identity_per_lookuptable_update_best_path_to_h(id, table, best, h, bid);
        }
    }

    void per_identity_foreach_lookuptable_update_all_best_paths(
        IdentityData id,
        Gee.List<LookupTable> tables,
        int bid)
    {
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
        foreach (LookupTable table in tables) if (! table.pkt_egress) neighbors.add(table.pkt_from);

        HashMap<HCoord, HashMap<string, BestRouteToDest>>
            all_best_routes = new HashMap<HCoord, HashMap<string, BestRouteToDest>>(
            (a) => a.lvl*100+a.pos,  /* hash_func */
            (a, b) => a.equals(b));  /* equal_func */
        for (int lvl = levels - 1; lvl >= subnetlevel; lvl--)
         for (int pos = 0; pos < _gsizes[lvl]; pos++)
         if (id.my_naddr.pos[lvl] != pos)
        {
            HCoord h = new HCoord(lvl, pos);
            // Retrieve all routes towards `h`.
            Gee.List<IQspnNodePath> paths;
            try {
                paths = qspn_mgr.get_paths_to(h);
            } catch (QspnBootstrapInProgressError e) {
                paths = new ArrayList<IQspnNodePath>();
            }

            // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
            all_best_routes[h] = find_best_route_to_dest_foreach_table(paths, neighbors);
        }
        foreach (LookupTable table in tables)
            per_identity_per_lookuptable_update_all_best_paths(id, table, all_best_routes, bid);
    }

    void per_identity_foreach_lookuptable_update_best_path_to_h(
        IdentityData id,
        HCoord h,
        int bid)
    {
        if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.

        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        // Compute list of neighbors.
        Gee.List<NeighborData> neighbors = all_neighbors(id, true);

        // Compute list of tables.
        ArrayList<LookupTable> tables = new ArrayList<LookupTable>();
        if (id.network_namespace == "") tables.add(new LookupTable.egress("ntk"));
        foreach (NeighborData neighbor in neighbors)
            tables.add(new LookupTable.forwarding(neighbor.tablename, neighbor));

        // Retrieve all routes towards `h`.
        Gee.List<IQspnNodePath> paths;
        try {
            paths = qspn_mgr.get_paths_to(h);
        } catch (QspnBootstrapInProgressError e) {
            paths = new ArrayList<IQspnNodePath>();
        }

        // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
        HashMap<string, BestRouteToDest> best_route_foreach_table = find_best_route_to_dest_foreach_table(paths, neighbors);

        foreach (LookupTable table in tables)
        {
            BestRouteToDest? best = null;
            if (best_route_foreach_table.has_key(table.tablename)) best = best_route_foreach_table[table.tablename];
            per_identity_per_lookuptable_update_best_path_to_h(id, table, best, h, bid);
        }
    }

    void check_first_etp_from_arcs(IdentityData id, int bid)
    {
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        // First iteration to gather lookup-tables
        ArrayList<LookupTable> update_tables = new ArrayList<LookupTable>();
        foreach (IdentityArc ia in id.identity_arcs.values) if (ia.qspn_arc != null)
        {
            // Module Qspn has received already at least one ETP from this arc?
            if (qspn_mgr.get_naddr_for_arc(ia.qspn_arc) != null)
            {
                // It is first ETP?
                if (ia.rule_added == false) /*it is a `maybe boolean`*/
                {
                    NeighborData neighbor = get_neighbor(id, ia);
                    update_tables.add(new LookupTable.forwarding(neighbor.tablename, neighbor));
                }
            }
        }
        // then update best routes for all.
        per_identity_foreach_lookuptable_update_all_best_paths(id, update_tables, bid);

        // Second iteration to add rules
        foreach (IdentityArc ia in id.identity_arcs.values) if (ia.qspn_arc != null)
        {
            // Module Qspn has received already at least one ETP from this arc?
            if (qspn_mgr.get_naddr_for_arc(ia.qspn_arc) != null)
            {
                // It is first ETP?
                if (ia.rule_added == false) /*it is a `maybe boolean`*/
                {
                    string ns = id.network_namespace;
                    ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
                    if (ns != "") prefix_cmd_ns.add_all_array({
                        @"ip", @"netns", @"exec", @"$(ns)"});
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"rule", @"add", @"fwmark", @"$(ia.tid)", @"table", @"$(ia.tablename)"});
                    cm.single_command_in_block(bid, cmd);

                    ia.rule_added = true;
                }
            }
        }
    }

    void per_identity_update_all_routes(IdentityData id)
    {
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

    class UpdateAllRoutesTasklet : Object, ITaskletSpawnable
    {
        public void * func()
        {
            while (true)
            {
                tasklet.ms_wait(10 * 60 * 1000);
                // iterate all identities (double check because the operations are lengthy)
                ArrayList<int> local_identities_keys = new ArrayList<int>();
                local_identities_keys.add_all(local_identities.keys);
                foreach (int i in local_identities_keys) if (local_identities.has_key(i))
                {
                    IdentityData id = local_identities[i];
                    per_identity_update_all_routes(id);
                }
            }
        }
    }
}
