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

        /* Single command
        ** 
        */

        public void single_command(string cmd, bool wait=true)
        {
            SingleCommandTasklet ts = new SingleCommandTasklet();
            ts.cm_t = this;
            ts.cmd = cmd;
            command_dispatcher.dispatch(ts, wait);
        }
        private void tasklet_single_command(string cmd)
        {
            try {
                TaskletCommandResult com_ret = tasklet.exec_command(cmd);
                if (com_ret.exit_status != 0)
                    error_in_command(cmd, com_ret.stdout, com_ret.stderr);
            } catch (Error e) {error("Unable to spawn a command");}
        }
        class SingleCommandTasklet : Object, ITaskletSpawnable
        {
            public Commander cm_t;
            public string cmd;
            public void * func()
            {
                cm_t.tasklet_single_command(cmd);
                return null;
            }
        }

        /* Block of commands
        ** 
        */

        private HashMap<int, BeginBlockTasklet> blocks;
        private int next_block_id = 0;
        public int begin_block()
        {
            int block_id = next_block_id++;
            blocks[block_id] = new BeginBlockTasklet(this);
            command_dispatcher.dispatch(blocks[block_id]); // does not wait
            return block_id;
        }
        private class BeginBlockTasklet : Object, ITaskletSpawnable
        {
            public BeginBlockTasklet(Commander cm_t)
            {
                this.cm_t = cm_t;
                ch = tasklet.get_channel();
                cmds = new ArrayList<string>();
            }

            private Commander cm_t;
            private IChannel ch;
            private ArrayList<string> cmds;
            private bool wait;

            public void single_command_in_block(string cmd)
            {
                cmds.add(cmd);
            }

            public void end_block(bool wait)
            {
                this.wait = wait;
                if (wait)
                {
                    ch.send(0);
                    ch.recv();
                }
                else
                {
                    ch.send_async(0);
                }
            }

            public void * func()
            {
                ch.recv();
                foreach (string cmd in cmds) cm_t.tasklet_single_command(cmd);
                if (wait) ch.send(0);
                return null;
            }
        }

        public void single_command_in_block(int block_id, string cmd)
        {
            assert(blocks.has_key(block_id));
            blocks[block_id].single_command_in_block(cmd);
        }

        public void end_block(int block_id, bool wait=true)
        {
            assert(blocks.has_key(block_id));
            blocks[block_id].end_block(wait);
            blocks.unset(block_id);
        }

        /* Table-names management
        ** 
        */

        private const string RT_TABLES = "/etc/iproute2/rt_tables";
        private static ArrayList<int> free_tid;
        private static HashMap<string, int> mac_tid;
        private static void init_table_names()
        {
            free_tid = new ArrayList<int>();
            for (int i = 250; i >= 200; i--) free_tid.add(i);
            mac_tid = new HashMap<string, int>();
        }

        public void get_tid(string peer_mac, out int tid, out string tablename)
        {
            tablename = @"ntk_from_$(peer_mac)";
            if (mac_tid.has_key(peer_mac))
            {
                tid = mac_tid[peer_mac];
                return;
            }
            assert(! free_tid.is_empty);
            tid = free_tid.remove_at(0);
            mac_tid[peer_mac] = tid;
            string cmd = @"sed -i 's/$(tid) reserved_ntk_from_$(tid)/$(tid) $(tablename)/' $(RT_TABLES)";
            single_command(cmd);
        }

        public void release_tid(string peer_mac, int tid)
        {
            string tablename = @"ntk_from_$(peer_mac)";
            assert(! (tid in free_tid));
            assert(mac_tid.has_key(peer_mac));
            assert(mac_tid[peer_mac] == tid);
            free_tid.insert(0, tid);
            mac_tid.unset(peer_mac);
            string cmd = @"sed -i 's/$(tid) $(tablename)/$(tid) reserved_ntk_from_$(tid)/' $(RT_TABLES)";
            single_command(cmd);
        }
    }
}
