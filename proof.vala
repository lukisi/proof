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
    string naddr;
    string gsizes;
    [CCode (array_length = false, array_null_terminated = true)]
    string[] interfaces;
    bool accept_anonymous_requests;
    bool no_anonymize;

    ITasklet tasklet;
    int main(string[] args)
    {
        accept_anonymous_requests = false; // default
        no_anonymize = false; // default
        OptionContext oc = new OptionContext("<topology> <address>");
        OptionEntry[] entries = new OptionEntry[4];
        int index = 0;
        entries[index++] = {"interfaces", 'i', 0, OptionArg.STRING_ARRAY, ref interfaces, "Interface (e.g. -i eth1). You can use it multiple times.", null};
        entries[index++] = {"serve-anonymous", 'k', 0, OptionArg.NONE, ref accept_anonymous_requests, "Accept anonymous requests", null};
        entries[index++] = {"no-anonymize", 'j', 0, OptionArg.NONE, ref no_anonymize, "Disable anonymizer", null};
        entries[index++] = { null };
        oc.add_main_entries(entries, null);
        try {
            oc.parse(ref args);
        }
        catch (OptionError e) {
            print(@"Error parsing options: $(e.message)\n");
            return 1;
        }

        if (args.length < 3) error("You have to set your topology (args[1]) and address (args[2]).");
        gsizes = args[1];
        naddr = args[2];
        ArrayList<int> _naddr = new ArrayList<int>();
        ArrayList<int> _gsizes = new ArrayList<int>();
        ArrayList<string> _devs = new ArrayList<string>();
        foreach (string s_piece in naddr.split(".")) _naddr.insert(0, int.parse(s_piece));
        foreach (string s_piece in gsizes.split(".")) _gsizes.insert(0, int.parse(s_piece));
        foreach (string dev in interfaces) _devs.add(dev);
        if (_naddr.size != _gsizes.size) error("You have to use same number of levels");

        // Initialize tasklet system
        PthTaskletImplementer.init();
        tasklet = PthTaskletImplementer.get_tasklet_system();

        // Initialize known serializable classes
        // typeof(MyNodeID).class_peek();

        // TODO startup

        // start a tasklet to get commands from stdin.
        CommandLineInterfaceTasklet ts = new CommandLineInterfaceTasklet();
        tasklet.spawn(ts);

        // register handlers for SIGINT and SIGTERM to exit
        Posix.signal(Posix.SIGINT, safe_exit);
        Posix.signal(Posix.SIGTERM, safe_exit);
        // Main loop
        while (true)
        {
            tasklet.ms_wait(100);
            if (do_me_exit) break;
        }

        // TODO cleanup

        PthTaskletImplementer.kill();
        print("\nExiting.\n");
        return 0;
    }

    bool do_me_exit = false;
    void safe_exit(int sig)
    {
        // We got here because of a signal. Quick processing.
        do_me_exit = true;
    }

    class CommandLineInterfaceTasklet : Object, ITaskletSpawnable
    {
        public void * func()
        {
            try {
                while (true)
                {
                    print("Ok> ");
                    uint8 buf[256];
                    size_t len = tasklet.read(0, (void*)buf, buf.length);
                    if (len > 255) error("Error during read of CLI: line too long");
                    string line = (string)buf;
                    if (line.has_suffix("\n")) line = line.substring(0, line.length-1);
                    ArrayList<string> _args = new ArrayList<string>();
                    foreach (string s_piece in line.split(" ")) _args.add(s_piece);
                    if (_args.size == 0)
                    {}
                    else if (_args[0] == "quit" && _args.size == 1)
                    {
                        do_me_exit = true;
                    }
                    else if (_args[0] == "show_linklocals" && _args.size == 1)
                    {
                        show_linklocals();
                    }
                    else if (_args[0] == "help" && _args.size == 1)
                    {
                        print("""
Command list:

> show_linklocals
  List current link-local addresses

> help
  Shows this menu.

> quit
  Exits. You can also press <ctrl-C>.

""");
                    }
                    else
                    {
                        print("CLI: unknown command\n");
                    }
                }
            } catch (Error e) {
                error(@"Error during read of CLI: $(e.message)");
            }
        }
    }

    void show_linklocals()
    {
        error("not implemented yet");
    }
}

