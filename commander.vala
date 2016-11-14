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
        private static ArrayList<int> free_tid;
        private static HashMap<string, int> mac_tid;
        private static void init_table_names()
        {
            free_tid = new ArrayList<int>();
            mac_tid = new HashMap<string, int>();
        }
    }
}
