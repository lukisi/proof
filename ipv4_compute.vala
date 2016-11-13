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
    string ip_global_node(Gee.List<int> n_addr)
    {
        // 1·2·3·4·5 in public-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        int32 ip = 0;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_anonymizing_node(Gee.List<int> n_addr)
    {
        // 1·2·3·4·5 in anon-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        int32 ip = 2;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_global_gnode(Gee.List<int> n_addr, int n_level)
    {
        // 1·2·3·* in public-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        int32 ip = 0;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c >= n_level) ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string ip_anonymizing_gnode(Gee.List<int> n_addr, int n_level)
    {
        // 1·2·3·* in anon-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < levels; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        int32 ip = 2;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c >= n_level) ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string ip_internal_node(Gee.List<int> n_addr, int inside_level)
    {
        // *·3·4·5 in public-range
        // Used in order to set its own address. Or to compute address to return from andna_resolv.
        assert(n_addr.size == levels);
        for (int l = 0; l < inside_level; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(inside_level >= 1);
        assert(inside_level < levels);
        int32 ip = 1;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c == levels - 1) ip += inside_level;
            else if (c >= inside_level) {}
            else ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        string ret = @"10.$(i2).$(i1).$(i0)";
        return ret;
    }

    string ip_internal_gnode(Gee.List<int> n_addr, int n_level, int inside_level)
    {
        // *·3·* in public-range
        // Used to set a route to a destination.
        assert(n_addr.size == levels);
        for (int l = n_level; l < inside_level; l++)
        {
            assert(n_addr[l] < _gsizes[l]);
            assert(n_addr[l] >= 0);
        }
        assert(n_level >= 0);
        assert(n_level < levels);
        assert(inside_level > n_level);
        assert(inside_level < levels);
        int32 ip = 1;
        for (int c = levels - 1; c >= 0; c--)
        {
            ip <<= _g_exp[c];
            if (c == levels - 1) ip += inside_level;
            else if (c >= inside_level) {}
            else if (c < n_level) {}
            else ip += n_addr[c];
        }
        int i0 = ip & 255;
        ip >>= 8;
        int i1 = ip & 255;
        ip >>= 8;
        int i2 = ip;
        int sum = 0;
        for (int k = 0; k <= n_level - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum;
        string ret = @"10.$(i2).$(i1).$(i0)/$(prefix)";
        return ret;
    }

    string compute_ip_whole_network()
    {
        int sum = 0;
        for (int k = 0; k <= levels - 1; k++) sum += _g_exp[k];
        int prefix = 32 - sum - 2;
        string ret = @"10.0.0.0/$(prefix)";
        return ret;
    }

    Gee.List<string> compute_ip_all_possible_destinations(Naddr own_naddr)
    {
        ArrayList<string> ret = new ArrayList<string>();
        for (int lvl = 0; lvl < levels; lvl++) for (int pos = 0; pos < _gsizes[lvl]; pos++) if (own_naddr.pos[lvl] != pos)
        {
            ret.add_all(compute_ip_one_destination(new HCoord(lvl, pos), own_naddr));
        }
        return ret;
    }

    Gee.List<string> compute_ip_one_destination(HCoord h, Naddr own_naddr)
    {
        ArrayList<string> ret = new ArrayList<string>();

        // Compute Netsukuku address of `h`.
        ArrayList<int> h_addr = new ArrayList<int>();
        h_addr.add_all(own_naddr.pos);
        h_addr[h.lvl] = h.pos;
        for (int i = 0; i < h.lvl; i++) h_addr[i] = -1;

        // Operations now are based on my own Netsukuku address:
        int real_up_to = own_naddr.get_real_up_to();
        if (real_up_to == levels-1)
        {
            ret.add_all(compute_ip_one_real_destination(h_addr, h.lvl, own_naddr));
        }
        else
        {
            int virtual_up_to = own_naddr.get_virtual_up_to();
            if (h.lvl < virtual_up_to)
            {
                ret.add_all(compute_ip_one_virtual_destination(h_addr, h.lvl, own_naddr));
            }
            else
            {
                ret.add_all(compute_ip_one_real_destination(h_addr, h.lvl, own_naddr));
            }
        }
        return ret;
    }

    Gee.List<string> compute_ip_one_real_destination(ArrayList<int> h_addr, int h_lvl, Naddr own_naddr)
    {
        ArrayList<string> ret = new ArrayList<string>();
        // Global.
        ret.add(ip_global_gnode(h_addr, h_lvl));
        // Anonymizing.
        ret.add(ip_anonymizing_gnode(h_addr, h_lvl));
        // Internals. In this case they are guaranteed to be valid.
        for (int t = h_lvl + 1; t <= levels - 1; t++)
        {
            ret.add(ip_internal_gnode(h_addr, h_lvl, t));
        }
        return ret;
    }

    Gee.List<string> compute_ip_one_virtual_destination(ArrayList<int> h_addr, int h_lvl, Naddr own_naddr)
    {
        ArrayList<string> ret = new ArrayList<string>();
        // Internals. In this case they MUST be checked.
        bool invalid_found = false;
        for (int t = h_lvl + 1; t <= levels - 1; t++)
        {
            for (int n_lvl = h_lvl + 1; n_lvl <= t - 1; n_lvl++)
            {
                if (h_addr[n_lvl] >= _gsizes[n_lvl])
                {
                    invalid_found = true;
                    break;
                }
            }
            if (invalid_found) break; // The higher levels will be invalid too.
            ret.add(ip_internal_gnode(h_addr, h_lvl, t));
        }
        return ret;
    }
}
