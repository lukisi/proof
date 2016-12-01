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
        public HCoord h;
    }

    class BestRoute : Object
    {
        public string gw;
        public string dev;
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

    void update_best_paths_per_identity(IdentityData id, HCoord h, int bid)
    {
        QspnManager qspn_mgr = (QspnManager)identity_mgr.get_identity_module(id.nodeid, "qspn");
        if (h.pos >= _gsizes[h.lvl]) return; // ignore virtual destination.
        print(@"Debug: IdentityData #$(id.local_identity_index): update_best_path for h ($(h.lvl), $(h.pos)): started.\n");
        // change the route. place current best path to `h`. if none, then change the path to 'unreachable'.

        // Retrieve all routes towards `h`.
        Gee.List<IQspnNodePath> paths;
        try {
            paths = qspn_mgr.get_paths_to(h);
        } catch (QspnBootstrapInProgressError e) {
            paths = new ArrayList<IQspnNodePath>();
        }

        // Compute Netsukuku address of `h`.
        ArrayList<int> h_addr = new ArrayList<int>();
        h_addr.add_all(id.my_naddr.pos);
        h_addr[h.lvl] = h.pos;
        for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

        // Compute list of neighbors.
        ArrayList<NeighborData> neighbors = new ArrayList<NeighborData>();
        foreach (IdentityArc ia in id.my_identityarcs) if (ia.qspn_arc != null)
        {
            Arc arc = ((IdmgmtArc)ia.arc).arc;
            IQspnNaddr? _neighbour_naddr = qspn_mgr.get_naddr_for_arc(ia.qspn_arc);
            if (_neighbour_naddr == null) continue;
            Naddr neighbour_naddr = (Naddr)_neighbour_naddr;
            INeighborhoodArc neighborhood_arc = arc.neighborhood_arc;
            NeighborData neighbor = new NeighborData();
            neighbor.mac = neighborhood_arc.neighbour_mac;
            neighbor.h = id.my_naddr.i_qspn_get_coord_by_address(neighbour_naddr);
            neighbors.add(neighbor);
        }

        // Find best routes towards `h` for table 'ntk' and for tables 'ntk_from_<MAC>'
        HashMap<string, BestRoute> best_routes = find_best_route_foreach_table(paths, neighbors);

        // Update best route in each table of our network namespace
        string ns = id.network_namespace;
        ArrayList<string> prefix_cmd_ns = new ArrayList<string>();
        if (ns != "") prefix_cmd_ns.add_all_array({
            @"ip", @"netns", @"exec", @"$(ns)"});
        DestinationIPSet h_ip_set = id.destination_ip_set[h.lvl][h.pos];
        if (h_ip_set.global != "")
        {
            assert(h_ip_set.anonymous != "");
            if (id.network_namespace == "")
            {
                string tablename = "ntk";
                if (best_routes.has_key("main"))
                {
                    // set route global
                    BestRoute best = best_routes["main"];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.global)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                    if (id.local_ip_set.global != "") cmd.add_all_array({@"src", @"$(id.local_ip_set.global)"});
                    cm.single_command_in_block(bid, cmd);
                    // set route anonymous
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.anonymous)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                    if (id.local_ip_set.global != "") cmd.add_all_array({@"src", @"$(id.local_ip_set.global)"});
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
            foreach (NeighborData neighbor in neighbors)
            {
                int tid;
                string tablename;
                tn.get_table(bid, neighbor.mac, out tid, out tablename);
                if (best_routes.has_key(neighbor.mac))
                {
                    // set route global
                    BestRoute best = best_routes[neighbor.mac];
                    ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.global)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                    cm.single_command_in_block(bid, cmd);
                    // set route anonymous
                    cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                    cmd.add_all_array({
                        @"ip", @"route", @"change",
                        @"$(h_ip_set.anonymous)", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
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
        }
        for (int k = levels - 1; k > h.lvl; k--)
        {
            if (h_ip_set.intern[k] != "")
            {
                if (id.network_namespace == "")
                {
                    string tablename = "ntk";
                    if (best_routes.has_key("main"))
                    {
                        // set route intern
                        BestRoute best = best_routes["main"];
                        ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"change",
                            @"$(h_ip_set.intern[k])", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
                        if (id.local_ip_set.intern[k] != "") cmd.add_all_array({@"src", @"$(id.local_ip_set.intern[k])"});
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
                foreach (NeighborData neighbor in neighbors)
                {
                    int tid;
                    string tablename;
                    tn.get_table(bid, neighbor.mac, out tid, out tablename);
                    if (best_routes.has_key(neighbor.mac))
                    {
                        // set route intern
                        BestRoute best = best_routes[neighbor.mac];
                        ArrayList<string> cmd = new ArrayList<string>(); cmd.add_all(prefix_cmd_ns);
                        cmd.add_all_array({
                            @"ip", @"route", @"change",
                            @"$(h_ip_set.intern[k])", @"table", @"$(tablename)", @"via", @"$(best.gw)", @"dev", @"$(best.dev)"});
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
    }

    HashMap<string, BestRoute>
    find_best_route_foreach_table(
        Gee.List<IQspnNodePath> paths,
        ArrayList<NeighborData> neighbors)
    {
        HashMap<string, BestRoute> best_routes = new HashMap<string, BestRoute>();
        foreach (IQspnNodePath path in paths)
        {
            QspnArc path_arc = (QspnArc)path.i_qspn_get_arc();
            string path_dev = path_arc.arc.neighborhood_arc.nic.dev;
            string gw = path_arc.arc.neighborhood_arc.neighbour_nic_addr;
            if (best_routes.is_empty)
            {
                string k = "main";
                // absolute best.
                BestRoute r = new BestRoute();
                r.gw = gw;
                r.dev = path_dev;
                best_routes[k] = r;
            }
            bool completed = true;
            foreach (NeighborData neighbor in neighbors)
            {
                // is it best without neighbor?
                string k = neighbor.mac;
                // best_routes contains k?
                if (best_routes.has_key(k)) continue;
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
                BestRoute r = new BestRoute();
                r.gw = gw;
                r.dev = path_dev;
                best_routes[k] = r;
            }
            if (completed) break;
        }
        return best_routes;
    }
}
