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
    class NeighborData : Object
    {
        public string mac;
        public string tablename;
        public HCoord h;
    }

    ArrayList<NeighborData> find_neighbors(IdentityData id, QspnManager qspn_mgr, int bid)
    {
        // Compute list of neighbors.
        ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
        foreach (IdentityArc ia in id.my_identityarcs) if (ia.qspn_arc != null)
        {
            NeighborData neighbor = new NeighborData();
            IQspnNaddr? neighbour_naddr = qspn_mgr.get_naddr_for_arc(ia.qspn_arc);
            if (neighbour_naddr == null) continue;
            neighbor.h = id.my_naddr.i_qspn_get_coord_by_address(neighbour_naddr);
            neighbor.mac = ia.id_arc.get_peer_mac();
            int tid;
            tn.get_table(bid, neighbor.mac, out tid, out neighbor.tablename);
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
        ArrayList<NeighborData> neighbors)
    {
        HashMap<string, BestRouteToDest> best_route_foreach_table = new HashMap<string, BestRouteToDest>();
        foreach (IQspnNodePath path in paths)
        {
            QspnArc path_arc = (QspnArc)path.i_qspn_get_arc();
            string path_dev = path_arc.arc.neighborhood_arc.nic.dev;
            string gw = path_arc.arc.neighborhood_arc.neighbour_nic_addr;
            if (best_route_foreach_table.is_empty)
            {
                // absolute best.
                BestRouteToDest r = new BestRouteToDest();
                r.gw = gw;
                r.dev = path_dev;
                best_route_foreach_table["ntk"] = r;
            }
            bool completed = true;
            foreach (NeighborData neighbor in neighbors)
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

    void per_identity_per_table_update_best_path_to_h(
        IdentityData id,
        string tablename,
        HashMap<string, BestRouteToDest> best_route_foreach_table,
        HCoord h,
        int bid)
    {
        string ns = id.network_namespace;
        ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
        if (ns != "") prefix_cmd_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(ns)"});
        DestinationIPSet h_ip_set = id.destination_ip_set[h.lvl][h.pos];
        bool egress_table = tablename == "ntk";
        if (egress_table) assert(ns == "");
        if (h_ip_set.global != "")
        {
            assert(h_ip_set.anonymous != "");
            if (best_route_foreach_table.has_key(tablename))
            {
                // set route global
                BestRouteToDest best = best_route_foreach_table[tablename];
                ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"$(h_ip_set.global)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                if (egress_table)
                 if (id.local_ip_set.global != "")
                    cmd.add_all_array({@"src", @"$(id.local_ip_set.global)"});
                cm.single_command_in_block(bid, cmd);
                // set route anonymous
                cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                cmd.add_all_array({
                    @"ip", @"route", @"change",
                    @"$(h_ip_set.anonymous)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                if (egress_table)
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
                if (best_route_foreach_table.has_key(tablename))
                {
                    // set route intern
                    BestRouteToDest best = best_route_foreach_table[tablename];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.intern[k])", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                    if (egress_table)
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

    void per_identity_per_table_update_all_best_paths(
        IdentityData id,
        string tablename,
        HashMap<HCoord, HashMap<string, BestRouteToDest>> all_best_routes,
        int bid)
    {
        for (int lvl = levels - 1; lvl >= subnetlevel; lvl--)
         for (int pos = 0; pos < _gsizes[lvl]; pos++)
         if (id.my_naddr.pos[lvl] != pos)
        {
            HCoord h = new HCoord(lvl, pos);
            per_identity_per_table_update_best_path_to_h(id, tablename, all_best_routes[h], h, bid);
        }
    }

    void per_identity_foreach_table_update_all_best_paths(
        IdentityData id,
        int bid,
        bool only_ntk=false)
    {
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        // Compute list of neighbors.
        ArrayList<NeighborData> neighbors = find_neighbors(id, qspn_mgr, bid);

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
        if (id.network_namespace == "")
            per_identity_per_table_update_all_best_paths(id, "ntk", all_best_routes, bid);
        if (!only_ntk) foreach (NeighborData neighbor in neighbors)
            per_identity_per_table_update_all_best_paths(id, neighbor.tablename, all_best_routes, bid);
    }

    void per_identity_foreach_table_update_best_path_to_h(
        IdentityData id,
        HCoord h,
        int bid)
    {
        if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.

        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");

        // Compute list of neighbors.
        ArrayList<NeighborData> neighbors = find_neighbors(id, qspn_mgr, bid);

        // Retrieve all routes towards `h`.
        Gee.List<IQspnNodePath> paths;
        try {
            paths = qspn_mgr.get_paths_to(h);
        } catch (QspnBootstrapInProgressError e) {
            paths = new ArrayList<IQspnNodePath>();
        }

        // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
        HashMap<string, BestRouteToDest> best_route_foreach_table = find_best_route_to_dest_foreach_table(paths, neighbors);

        if (id.network_namespace == "")
            per_identity_per_table_update_best_path_to_h(id, "ntk", best_route_foreach_table, h, bid);
        foreach (NeighborData neighbor in neighbors)
            per_identity_per_table_update_best_path_to_h(id, neighbor.tablename, best_route_foreach_table, h, bid);
    }

    void update_rules(IdentityData id, int bid)
    {
        // Every time the Qspn module updates its map, it means an ETP is being processed. Then this function gets called.

        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");
        foreach (IdentityArc ia in id.my_identityarcs) if (ia.qspn_arc != null)
        {
            //  Module Qspn has received already at least one ETP from this arc?
            if (qspn_mgr.get_naddr_for_arc(ia.qspn_arc) != null)
            {
                if (! ia.rule_added)
                {
                    int tid;
                    string tablename;
                    tn.get_table(bid, ia.id_arc.get_peer_mac(), out tid, out tablename);
                    string ns = id.network_namespace;
                    ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
                    if (ns != "") prefix_cmd_ns.add_all_array({
                        @"ip", @"netns", @"exec", @"$(ns)"});
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"rule", @"add", @"fwmark", @"$(tid)", @"table", @"$(tablename)"});
                    cm.single_command_in_block(bid, cmd);

                    ia.rule_added = true;
                }
            }
        }
    }
}
