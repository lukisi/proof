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
using TaskletSystem;

namespace ProofOfConcept
{
    [NoReturn]
    internal void error_in_command(string cmd, string stdout, string stderr)
    {
        print("Error in command:\n");
        print(@"   $(cmd)\n");
        print("command stdout =======\n");
        print(@"$(stdout)\n");
        print("======================\n");
        print("command stderr =======\n");
        print(@"$(stderr)\n");
        print("======================\n");
        error(@"Error in command: `$(cmd)`");
    }

    class Commander : Object
    {
        public static Commander get_singleton()
        {
            if (singleton == null) singleton = new Commander();
            return singleton;
        }

        private static Commander singleton;

        private Commander()
        {
            init_table_names();
            command_dispatcher = tasklet.create_dispatchable_tasklet();
        }

        private DispatchableTasklet command_dispatcher;

        /* Table-names management
        ** 
        */

        private const string RT_TABLES = "/etc/iproute2/rt_tables";
        private static HashMap<string, int> table_references;
        private static HashMap<string, int> table_number;
        private static void init_table_names()
        {
            table_references = new HashMap<string, int>();
            table_number = new HashMap<string, int>();
        }

        /** Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number and line.
          * Otherwise report all busy numbers.
          */
        private void find_tablename(string tablename, out int num, out string line, out ArrayList<int> busy_nums)
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

        /** Write a new record on /etc/iproute2/rt_tables.
          */
        private void add_tablename(string tablename, int new_num)
        {
            print(@"Adding table $(tablename) as number $(new_num) in file $(RT_TABLES)...\n");
            string to_add = @"\n$(new_num)\t$(tablename)\n";
            // a path
            File fout = File.new_for_path(RT_TABLES);
            // add "to_add" to file
            try {
                FileOutputStream fos = fout.append_to(FileCreateFlags.NONE);
                fos.write(to_add.data);
            } catch (Error e) {assert_not_reached();}
            print(@"Added table $(tablename).\n");
        }

        /** Check the list of tables in /etc/iproute2/rt_tables.
          * If <tablename> is already there, get its number and line.
          * Otherwise report all busy numbers.
          */
        private void remove_tablename(string tablename)
        {
            int num;
            string line;
            ArrayList<int> busy_nums;
            find_tablename(tablename, out num, out line, out busy_nums);
            if (num == -1)
            {
                // not present
                error(@"remove_tablename: table $(tablename) not present");
            }
            // remove record $(line) from file
            print(@"Removing table $(tablename) from file $(RT_TABLES)...\n");
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
            print(@"Removed table $(tablename).\n");
        }
    }
}
