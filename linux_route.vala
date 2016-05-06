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
    class LinuxRoute : Object
    {
        public LinuxRoute(string network_namespace)
        {
            ns = network_namespace;
            my_destinations_dispatchers = new HashMap<string, DispatchableTasklet>();
            local_addresses = new ArrayList<string>();
        }

        private string ns;
        private HashMap<string, DispatchableTasklet> my_destinations_dispatchers;
        ArrayList<string> local_addresses;

        /* Handle change of network namespace
        **
        */

        public void change_namespace(string new_ns)
        {
            remove_addresses();
            ns = new_ns;
            // TODO more things?
        }

        /* Route table management
        ** 
        */

        private const string RT_TABLES = "/etc/iproute2/rt_tables";

        /** Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number and line.
          * Otherwise report all busy numbers.
          */
        public void scan_tables_list(string tablename, out int num, out string line, out ArrayList<int> busy_nums)
        {
            num = -1;
            line = "";
            busy_nums = new ArrayList<int>();
            // a path
            File ftable = File.new_for_path(RT_TABLES);
            // load content
            uint8[] rt_tables_content_arr;
            try {
                ftable.load_contents(null, out rt_tables_content_arr, null);
            } catch (Error e) {assert_not_reached();}
            string rt_tables_content = (string)rt_tables_content_arr;
            string[] lines = rt_tables_content.split("\n");
            foreach (string cur_line in lines)
            {
                if (cur_line.has_suffix(@" $(tablename)") || cur_line.has_suffix(@"\t$(tablename)"))
                {
                    string prefix = cur_line.substring(0, cur_line.length - tablename.length - 1);
                    // remove trailing blanks
                    while (prefix.has_suffix(" ") || prefix.has_suffix("\t"))
                        prefix = prefix.substring(0, prefix.length - 1);
                    // remove leading blanks
                    while (prefix.has_prefix(" ") || prefix.has_prefix("\t"))
                        prefix = prefix.substring(1);
                    num = int.parse(prefix);
                    line = cur_line;
                    break;
                }
                else
                {
                    string prefix = cur_line;
                    // remove leading blanks
                    while (prefix.has_prefix(" ") || prefix.has_prefix("\t"))
                        prefix = prefix.substring(1);
                    if (prefix.has_prefix("#")) continue;
                    // find next blank
                    int pos1 = prefix.index_of(" ");
                    int pos2 = prefix.index_of("\t");
                    if (pos1 == pos2) continue;
                    if (pos1 == -1 || pos1 > pos2) pos1 = pos2;
                    prefix = prefix.substring(0, pos1);
                    int busynum = int.parse(prefix);
                    busy_nums.add(busynum);
                }
            }
        }

        /** Create (or empty if it exists) a table <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number.
          * Otherwise find a free number and write a new record on /etc/iproute2/rt_tables.
          * Then empty the table (ip r flush table <tablename>).
          */
        public void create_table(string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                int new_num = 255;
                while (new_num >= 0)
                {
                    if (! (new_num in busy_nums)) break;
                    new_num--;
                }
                if (new_num < 0)
                {
                    error("no more free numbers in rt_tables: not implemented yet");
                }
                string to_add = @"\n$(new_num)\t$(tablename)\n";
                // a path
                File fout = File.new_for_path(RT_TABLES);
                // add "to_add" to file
                try {
                    FileOutputStream fos = fout.append_to(FileCreateFlags.NONE);
                    fos.write(to_add.data);
                } catch (Error e) {assert_not_reached();}
            }
            // emtpy the table
            try {
                string cmd = @"ip route flush table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        /** Remove (once emptied) a table <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number.
          * Otherwise abort.
          * Then empty the table (ip r flush table <tablename>).
          * Then remove the record from /etc/iproute2/rt_tables.
          */
        public void remove_table(string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"remove_table: table $(tablename) not present");
            }
            // emtpy the table
            try {
                string cmd = @"ip route flush table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
            // remove record $(line) from file
            string rt_tables_content;
            {
                // a path
                File ftable = File.new_for_path(RT_TABLES);
                // load content
                uint8[] rt_tables_content_arr;
                try {
                    ftable.load_contents(null, out rt_tables_content_arr, null);
                } catch (Error e) {assert_not_reached();}
                rt_tables_content = (string)rt_tables_content_arr;
            }
            string[] lines = rt_tables_content.split("\n");
            {
                string new_cont = "";
                foreach (string old_line in lines)
                {
                    if (old_line == line) continue;
                    new_cont += old_line;
                    new_cont += "\n";
                }
                // twice remove trailing new-line
                if (new_cont.has_suffix("\n")) new_cont = new_cont.substring(0, new_cont.length-1);
                if (new_cont.has_suffix("\n")) new_cont = new_cont.substring(0, new_cont.length-1);
                // replace into path
                File fout = File.new_for_path(RT_TABLES);
                try {
                    fout.replace_contents(new_cont.data, null, false, FileCreateFlags.NONE, null);
                } catch (Error e) {assert_not_reached();}
            }
        }

        /** Rule that a packet which is coming from <macaddr> and has to be forwarded
          * will search for its route in <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number <number>.
          * Otherwise abort.
          * Once we have the number, use "iptables" to set a MARK <number> to the packets
          * coming from this <macaddr>; and use "ip" to rule that those packets
          * search into table <tablename>
                iptables -t mangle -A PREROUTING -m mac --mac-source $macaddr -j MARK --set-mark $number
                ip rule add fwmark $number table $tablename
          */
        public void rule_coming_from_macaddr(string macaddr, string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"rule_coming_from_macaddr: table $(tablename) not present");
            }
            string pres;
            try {
                string cmd = @"ip rule list";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
                pres = com_ret.stdout;
            } catch (Error e) {error("Unable to spawn a command");}
            if (@" lookup $(tablename) " in pres) error(@"rule_coming_from_macaddr: rule for $(tablename) was already there");
            try {
                string cmd = @"iptables -t mangle -A PREROUTING -m mac --mac-source $(macaddr) -j MARK --set-mark $(num)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
            try {
                string cmd = @"ip rule add fwmark $(num) table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        /** Remove rule that a packet which is coming from <macaddr> and has to be forwarded
          * will search for its route in <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number <number>.
          * Otherwise abort.
          * Once we have the number, use "iptables" to remove set-mark and use "ip" to remove the rule fwmark.
                iptables -t mangle -D PREROUTING -m mac --mac-source $macaddr -j MARK --set-mark $number
                ip rule del fwmark $number table $tablename
          */
        public void remove_rule_coming_from_macaddr(string macaddr, string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"rule_coming_from_macaddr: table $(tablename) not present");
            }
            try {
                string cmd = @"iptables -t mangle -D PREROUTING -m mac --mac-source $(macaddr) -j MARK --set-mark $(num)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
            try {
                string cmd = @"ip rule del fwmark $(num) table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        /** Rule that a packet by default (in egress)
          * will search for its route in <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number <number>.
          * Otherwise abort.
          * Use "ip" to rule that all packets search into table <tablename>
                ip rule add table $tablename
          */
        public void rule_default(string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"rule_default: table $(tablename) not present");
            }
            string pres;
            try {
                string cmd = @"ip rule list";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
                pres = com_ret.stdout;
            } catch (Error e) {error("Unable to spawn a command");}
            if (@" lookup $(tablename) " in pres) error(@"rule_default: rule for $(tablename) was already there");
            try {
                string cmd = @"ip rule add table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        /** Remove rule that a packet by default (in egress)
          * will search for its route in <tablename>.
          *
          * Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number <number>.
          * Otherwise abort.
          * Use "ip" to remove rule that all packets search into table <tablename>
                ip rule del table $tablename
          */
        public void remove_rule_default(string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            scan_tables_list(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"remove_rule_default: table $(tablename) not present");
            }
            try {
                string cmd = @"ip rule del table $(tablename)";
                print(@"$(cmd)\n");
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        /* Routes management
        **
        */

        public void add_destination(string dest)
        {
            if (! my_destinations_dispatchers.has_key(dest))
            {
                my_destinations_dispatchers[dest] = tasklet.create_dispatchable_tasklet();
            }
            DispatchableTasklet dt = my_destinations_dispatchers[dest];
            AddDestinationTasklet ts = new AddDestinationTasklet();
            ts.t = this;
            ts.dest = dest;
            dt.dispatch(ts);
        }
        private void tasklet_add_destination(string dest)
        {
            // TODO add dest unreachable
        }
        class AddDestinationTasklet : Object, ITaskletSpawnable
        {
            public LinuxRoute t;
            public string dest;
            public void * func()
            {
                t.tasklet_add_destination(dest);
                return null;
            }
        }

        public void remove_destination(string dest)
        {
            if (! my_destinations_dispatchers.has_key(dest))
            {
                my_destinations_dispatchers[dest] = tasklet.create_dispatchable_tasklet();
            }
            DispatchableTasklet dt = my_destinations_dispatchers[dest];
            RemoveDestinationTasklet ts = new RemoveDestinationTasklet();
            ts.t = this;
            ts.dest = dest;
            dt.dispatch(ts, true);
            if (dt.is_empty()) my_destinations_dispatchers.unset(dest);
        }
        private void tasklet_remove_destination(string dest)
        {
            // TODO remove dest
        }
        class RemoveDestinationTasklet : Object, ITaskletSpawnable
        {
            public LinuxRoute t;
            public string dest;
            public void * func()
            {
                t.tasklet_remove_destination(dest);
                return null;
            }
        }

        // TODO         // change route to dest.

        /* Own address management
        **
        */

        public void add_address(string address, string dev)
        {
            string local_address = @"$(address)/32 dev $(dev)";
            assert(!(local_address in local_addresses));
            local_addresses.add(local_address);
            string cmd = @"ip address add $(address) dev $(dev)";
            if (ns != "") cmd = @"ip netns exec $(ns) $(cmd)";
            print(@"$(cmd)\n");
            try {
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error(@"$(com_ret.stderr)\n");
            } catch (Error e) {error("Unable to spawn a command");}
        }

        public void remove_addresses()
        {
            foreach (string local_address in local_addresses)
            {
                string cmd = @"ip address del $(local_address)";
                if (ns != "") cmd = @"ip netns exec $(ns) $(cmd)";
                print(@"$(cmd)\n");
                try {
                    TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                    if (com_ret.exit_status != 0)
                        error(@"$(com_ret.stderr)\n");
                } catch (Error e) {error("Unable to spawn a command");}
            }
            local_addresses.clear();
        }
    }
}

