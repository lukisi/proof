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
    string pipe_response;
    string pipe_commands;
    int server_fd_commands;
    int client_fd_response;

    void pipe_init()
    {
        string basedir = "/var/run";
        unowned string xdg_runtime_dir = Environment.get_variable("XDG_RUNTIME_DIR");
        if (!(xdg_runtime_dir == null || xdg_runtime_dir == "")) basedir = xdg_runtime_dir;
        if (basedir.has_suffix("/")) basedir = basedir.substring(0, basedir.length-1);
        int mode = (int)(Posix.S_IRWXU | Posix.S_IRGRP | Posix.S_IXGRP | Posix.S_IROTH | Posix.S_IXOTH);
        int r = DirUtils.create_with_parents(basedir, mode);
        if (r != 0) error(@"Couldn't create dir '$(basedir)'.");
        pipe_response = @"$(basedir)/qspnclient_pipe_response";
        pipe_commands = @"$(basedir)/qspnclient_pipe_commands";
    }

    size_t nonblock_read(int fd, void* b, size_t nbytes) throws Error
    {
        ssize_t result = Posix.read(fd, b, nbytes);
        if (result == -1)
        {
            if (errno == Posix.EAGAIN) return (size_t)0;
            report_error_posix_read();
        }
        return (size_t)result;
    }

    [NoReturn]
    void report_error_posix_read() throws Error
    {
        if (errno == Posix.EAGAIN)
            throw new FileError.FAILED(@"Posix.read returned EAGAIN");
        if (errno == Posix.EWOULDBLOCK)
            throw new FileError.FAILED(@"Posix.read returned EWOULDBLOCK");
        if (errno == Posix.EBADF)
            throw new FileError.FAILED(@"Posix.read returned EBADF");
        if (errno == Posix.ECONNREFUSED)
            throw new FileError.FAILED(@"Posix.read returned ECONNREFUSED");
        if (errno == Posix.EFAULT)
            throw new FileError.FAILED(@"Posix.read returned EFAULT");
        if (errno == Posix.EINTR)
            throw new FileError.FAILED(@"Posix.read returned EINTR");
        if (errno == Posix.EINVAL)
            throw new FileError.FAILED(@"Posix.read returned EINVAL");
        if (errno == Posix.ENOMEM)
            throw new FileError.FAILED(@"Posix.read returned ENOMEM");
        if (errno == Posix.ENOTCONN)
            throw new FileError.FAILED(@"Posix.read returned ENOTCONN");
        if (errno == Posix.ENOTSOCK)
            throw new FileError.FAILED(@"Posix.read returned ENOTSOCK");
        throw new FileError.FAILED(@"Posix.read returned -1, errno = $(errno)");
    }

    void server_open_pipe_commands()
    {
        int ret = Posix.mkfifo(pipe_commands, Posix.S_IRUSR | Posix.S_IWUSR);
        if (ret != 0 && Posix.errno == Posix.EEXIST)
        {
            error("Server is already in progress.");
        }
        if (ret != 0) error(@"Couldn't create pipe commands: Posix.errno = $(Posix.errno)");
        server_fd_commands = Posix.open(pipe_commands, Posix.O_RDONLY | Posix.O_NONBLOCK);
        if (server_fd_commands == -1) error(@"Couldn't open pipe commands: Posix.errno = $(Posix.errno)");
    }

    void client_open_pipe_response()
    {
        int ret = Posix.mkfifo(pipe_response, Posix.S_IRUSR | Posix.S_IWUSR);
        if (ret != 0 && Posix.errno == Posix.EEXIST)
        {
            error("Client: Another command is now in progress.");
        }
        if (ret != 0) error(@"Couldn't create pipe response: Posix.errno = $(Posix.errno)");
        client_fd_response = Posix.open(pipe_response, Posix.O_RDONLY | Posix.O_NONBLOCK);
        if (client_fd_response == -1) error(@"Couldn't open pipe response: Posix.errno = $(Posix.errno)");
    }

    void remove_pipe_commands()
    {
        Posix.close(server_fd_commands);
        Posix.unlink(pipe_commands);
    }

    void remove_pipe_response()
    {
        Posix.close(client_fd_response);
        Posix.unlink(pipe_response);
    }

    bool check_pipe_response()
    {
        return check_pipe(pipe_response);
    }

    bool check_pipe(string fname)
    {
        Posix.Stat sb;
        int ret = Posix.stat(fname, out sb);
        if (ret != 0 && Posix.errno == Posix.ENOENT) return false;
        if (ret != 0)
        {
            print(@"check_pipe($(fname)): stat: ret = $(ret)\n");
            print(@"stat: errno = $(Posix.errno)\n");
            switch (Posix.errno)
            {
                case Posix.EACCES:
                    print("EACCES\n");
                    break;
                case Posix.EBADF:
                    print("EBADF\n");
                    break;
                case Posix.EFAULT:
                    print("EFAULT\n");
                    break;
                case Posix.ELOOP:
                    print("ELOOP\n");
                    break;
                case Posix.ENAMETOOLONG:
                    print("ENAMETOOLONG\n");
                    break;
                default:
                    print("???\n");
                    break;
            }
            error(@"unexpected stat retcode");
        }
        if (Posix.S_ISFIFO(sb.st_mode)) return true;
        error(@"unexpected stat result from file $(fname)");
    }

    string read_command() throws Error
    {
        uint8 buf[256];
        size_t len = 0;
        while (true)
        {
            while (true)
            {
                size_t nb = nonblock_read(server_fd_commands, (void*)(((uint8*)buf)+len), 1);
                if (nb == 0)
                {
                    tasklet.ms_wait(2);
                }
                else
                {
                    len += nb;
                    break;
                }
            }
            if (buf[len-1] == '\n') break;
            if (len >= buf.length) error("command too long");
        }
        string line = (string)buf;
        line = line.substring(0, line.length-1);
        return line;
    }

    void write_response(string _res) throws Error
    {
        string res = _res + "\n";
        int fd_response = Posix.open(pipe_response, Posix.O_WRONLY);
        size_t remaining = res.length;
        uint8 *buf = res.data;
        while (remaining > 0)
        {
            size_t len = tasklet.write(fd_response, (void*)buf, remaining);
            remaining -= len;
            buf += len;
        }
        Posix.close(fd_response);
    }

    void write_block_response(string command_id, Gee.List<string> lines, int retval=0) throws Error
    {
        write_response(@"$(command_id) $(retval) $(lines.size)");
        foreach (string line in lines) write_response(line);
    }

    void write_empty_response(string command_id, int retval=0) throws Error
    {
        write_block_response(command_id, new ArrayList<string>(), retval);
    }

    void write_oneline_response(string command_id, string line, int retval=0) throws Error
    {
        write_block_response(command_id, new ArrayList<string>.wrap({line}), retval);
    }

    void write_command(string _res) throws Error
    {
        string res = _res + "\n";
        int fd_commands = Posix.open(pipe_commands, Posix.O_WRONLY);
        size_t remaining = res.length;
        uint8 *buf = res.data;
        while (remaining > 0)
        {
            size_t len = tasklet.write(fd_commands, (void*)buf, remaining);
            remaining -= len;
            buf += len;
        }
        Posix.close(fd_commands);
    }

    string read_response() throws Error
    {
        uint8 buf[256];
        size_t len = 0;
        while (true)
        {
            while (true)
            {
                size_t nb = nonblock_read(client_fd_response, (void*)(((uint8*)buf)+len), 1);
                if (nb == 0)
                {
                    tasklet.ms_wait(2);
                }
                else
                {
                    len += nb;
                    break;
                }
            }
            if (buf[len-1] == '\n') break;
            if (len >= buf.length) error("response too long");
        }
        string line = (string)buf;
        line = line.substring(0, line.length-1);
        return line;
    }
}
