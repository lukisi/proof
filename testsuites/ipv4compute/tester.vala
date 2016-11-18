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

namespace ProofOfConcept
{
    ArrayList<int> _gsizes;
    ArrayList<int> _g_exp;
    int levels;

    class ComputeTester : Object
    {
        public void set_up ()
        {
        }

        public void tear_down ()
        {
        }

        public void test_global()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string g1 = ip_global_node(new ArrayList<int>.wrap({1, 0, 1, 3}));
            // print(@"g1 = $(g1)\n");
            assert(g1 == "10.0.0.29");
            string g2 = ip_global_node(new ArrayList<int>.wrap({1, 0, 0, 1}));
            assert(g2 == "10.0.0.9");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string g3 = ip_global_node(new ArrayList<int>.wrap({234, 123, 15, 3}));
            assert(g3 == "10.63.123.234");
        }

        public void test_anon()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string a1 = ip_anonymizing_node(new ArrayList<int>.wrap({1, 0, 1, 3}));
            assert(a1 == "10.0.0.93");
            string a2 = ip_anonymizing_node(new ArrayList<int>.wrap({1, 0, 0, 1}));
            assert(a2 == "10.0.0.73");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string a3 = ip_anonymizing_node(new ArrayList<int>.wrap({234, 123, 15, 3}));
            assert(a3 == "10.191.123.234");
        }

        public void test_internal()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string i1 = ip_internal_node(new ArrayList<int>.wrap({1, 0, 1, 3}), 3);
            assert(i1 == "10.0.0.61");
            string i2 = ip_internal_node(new ArrayList<int>.wrap({1, 0, 0, 1}), 3);
            assert(i2 == "10.0.0.57");
            string i3 = ip_internal_node(new ArrayList<int>.wrap({1, 1, 1, 2}), 1);
            assert(i3 == "10.0.0.41");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string i4 = ip_internal_node(new ArrayList<int>.wrap({234, 123, 15, 3}), 3);
            assert(i4 == "10.127.123.234");
            string i5 = ip_internal_node(new ArrayList<int>.wrap({234, 123, 15, 3}), 0); // ntklocalhost
            assert(i5 == "10.64.0.0");
        }

        public void test_dest_global()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string g1 = ip_global_gnode(new ArrayList<int>.wrap({1, 0, 1, 3}),1);
            assert(g1 == "10.0.0.28/31");
            string g2 = ip_global_gnode(new ArrayList<int>.wrap({1, 0, 0, 1}),1);
            assert(g2 == "10.0.0.8/31");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string g3 = ip_global_gnode(new ArrayList<int>.wrap({234, 123, 15, 3}),1);
            assert(g3 == "10.63.123.0/24");
        }

        public void test_dest_anon()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string a1 = ip_anonymizing_gnode(new ArrayList<int>.wrap({1, 0, 1, 3}),1);
            assert(a1 == "10.0.0.92/31");
            string a2 = ip_anonymizing_gnode(new ArrayList<int>.wrap({1, 0, 0, 1}),1);
            assert(a2 == "10.0.0.72/31");
            string a_range = ip_anonymizing_gnode(new ArrayList<int>.wrap({1, 0, 0, 1}),4); // anonymousrange
            assert(a_range == "10.0.0.64/27");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string a3 = ip_anonymizing_gnode(new ArrayList<int>.wrap({234, 123, 15, 3}),1);
            assert(a3 == "10.191.123.0/24");
        }

        public void test_dest_internal()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            string i1 = ip_internal_gnode(new ArrayList<int>.wrap({1, 0, 1, 3}),1, 3);
            assert(i1 == "10.0.0.60/31");
            string i2 = ip_internal_gnode(new ArrayList<int>.wrap({1, 0, 0, 1}),1, 3);
            assert(i2 == "10.0.0.56/31");
            string i3 = ip_internal_gnode(new ArrayList<int>.wrap({1, 1, 1, 2}),1, 2);
            assert(i3 == "10.0.0.50/31");
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({256, 256, 16, 4});
            _g_exp = new ArrayList<int>.wrap({8, 8, 4, 2});
            string i4 = ip_internal_gnode(new ArrayList<int>.wrap({234, 123, 15, 3}),1, 3);
            assert(i4 == "10.127.123.0/24");
            string i5 = ip_internal_gnode(new ArrayList<int>.wrap({234, 123, 15, 3}),1, 2);
            assert(i5 == "10.96.123.0/24");
        }

        public void test_autonomous_subnet()
        {
            levels = 4;
            _gsizes = new ArrayList<int>.wrap({2, 2, 2, 4});
            _g_exp = new ArrayList<int>.wrap({1, 1, 1, 2});
            ArrayList<int> lamda_addr = new ArrayList<int>.wrap({0, 0, 1, 1});
            int subnetlevel = 1;

            /*
            ip address add 10.0.0.32 dev lo

            ip address add 10.0.0.12 dev eth1
            ip address add 10.0.0.76 dev eth1
            ip address add 10.0.0.60 dev eth1
            ip address add 10.0.0.48 dev eth1
            ip address add 10.0.0.40 dev eth1

            iptables -t nat -A PREROUTING -d 10.0.0.48/31 -j NETMAP --to 10.0.0.40/31
            iptables -t nat -A POSTROUTING -d 10.0.0.48/30 -s 10.0.0.40/31 -j NETMAP --to 10.0.0.48/31

            iptables -t nat -A PREROUTING -d 10.0.0.60/31 -j NETMAP --to 10.0.0.40/31
            iptables -t nat -A POSTROUTING -d 10.0.0.56/29 -s 10.0.0.40/31 -j NETMAP --to 10.0.0.60/31

            iptables -t nat -A PREROUTING -d 10.0.0.12/31 -j NETMAP --to 10.0.0.40/31
            iptables -t nat -A POSTROUTING -d 10.0.0.0/27  -s 10.0.0.40/31 -j NETMAP --to 10.0.0.12/31
            iptables -t nat -A PREROUTING -d 10.0.0.76/31 -j NETMAP --to 10.0.0.40/31
            iptables -t nat -A POSTROUTING -d 10.0.0.64/27 -s 10.0.0.40/31 -j NETMAP --to 10.0.0.12/31
            */

            // print(@"ip address add $(ip_internal_node(lamda_addr, 0)) dev lo\n");
            // print("\n");
            // print(@"ip address add $(ip_global_node(lamda_addr)) dev eth1\n");
            // print(@"ip address add $(ip_anonymizing_node(lamda_addr)) dev eth1\n");
            // print(@"ip address add $(ip_internal_node(lamda_addr, 3)) dev eth1\n");
            // print(@"ip address add $(ip_internal_node(lamda_addr, 2)) dev eth1\n");
            // print(@"ip address add $(ip_internal_node(lamda_addr, 1)) dev eth1\n");
            // print("\n");

            string range1 = ip_internal_gnode(lamda_addr, subnetlevel, subnetlevel);
            assert(range1 == "10.0.0.40/31");
            for (int i = subnetlevel; i < levels; i++)
            {
                if (i < levels-1)
                {
                    string range2 = ip_internal_gnode(lamda_addr, subnetlevel, i+1);
                    if (i == 1) assert(range2 == "10.0.0.48/31");
                    if (i == 2) assert(range2 == "10.0.0.60/31");
                    string range3 = ip_internal_gnode(lamda_addr, i+1, i+1);
                    if (i == 1) assert(range3 == "10.0.0.48/30");
                    if (i == 2) assert(range3 == "10.0.0.56/29");
                    // print(@"iptables -t nat -A PREROUTING -d $range2 -j NETMAP --to $range1\n");
                    // print(@"iptables -t nat -A POSTROUTING -d $range3 -s $range1 -j NETMAP --to $range2\n");
                    // print("\n");
                }
                else
                {
                    string range2 = ip_global_gnode(lamda_addr, subnetlevel);
                    assert(range2 == "10.0.0.12/31");
                    string range3 = ip_global_gnode(lamda_addr, levels);
                    assert(range3 == "10.0.0.0/27");
                    string range4 = ip_anonymizing_gnode(lamda_addr, subnetlevel);
                    assert(range4 == "10.0.0.76/31");
                    string range5 = ip_anonymizing_gnode(lamda_addr, levels);
                    assert(range5 == "10.0.0.64/27");
                    // print(@"iptables -t nat -A PREROUTING -d $range2 -j NETMAP --to $range1\n");
                    // print(@"iptables -t nat -A POSTROUTING -d $range3 -s $range1 -j NETMAP --to $range2\n");
                    // print(@"iptables -t nat -A PREROUTING -d $range4 -j NETMAP --to $range1\n");
                    // print(@"iptables -t nat -A POSTROUTING -d $range5 -s $range1 -j NETMAP --to $range2\n");
                }
            }
        }

        public static int main(string[] args)
        {
            GLib.Test.init(ref args);
            GLib.Test.add_func ("/ProofOfConcept/ComputeIP", () => {
                var x = new ComputeTester();
                x.set_up();
                x.test_global();
                x.test_anon();
                x.test_internal();
                x.test_dest_global();
                x.test_dest_anon();
                x.test_dest_internal();
                x.tear_down();
                x.test_autonomous_subnet();
            });
            GLib.Test.run();
            return 0;
        }
    }
}
