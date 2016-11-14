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
            string i5 = ip_internal_node(new ArrayList<int>.wrap({234, 123, 15, 3}), 0); //localhost
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
            });
            GLib.Test.run();
            return 0;
        }
    }
}
