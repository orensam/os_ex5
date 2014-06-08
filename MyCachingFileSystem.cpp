/*
 * MyCachingFileSystem.cpp
 *
 *  Created on: May 21, 2014
 *      Author: orensam
 */

#define FUSE_USE_VERSION 26

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/xattr.h>
#include <iostream>

#include <sys/time.h>
#include <unordered_map>
#include <algorithm>
#include <set>

#include <cstring>
#include <fstream>
#include <sstream>

#include <functional>

using namespace std;

// Return codes
static const int CODE_FAIL = -1;
static const int CODE_SUCCESS = 0;

// Error messages
static const string ERROR_USAGE = "usage: MyCachingFileSystem rootdir mountdir numberOfBlocks blockSize";
static const string ERROR_SYSTEM = "error in";
static const string ERROR_OPEN_LOG = "system error: couldn't open ioctloutput.log file";
static const string ERROR_WRITE_LOG = "system error: couldn't write to ioctloutput.log file";

// Filenames
#define LOG_FILE_NAME "ioctloutput.log"

// Allignment for posix_memallign
#define ALLIGN_OFFSET 4096

// Fuse's private data
#define CACHING_DATA ((struct caching_state *) fuse_get_context()->private_data)

/**
 * Returns a string representation of the given timeval.
 * The format is as specified in the exercise description.
 */
static string timeval_to_str(const timeval& tv)
{
	struct tm *nowtm;
	char tmbuf[64];
	char tmbuf2[64];

	nowtm = localtime(&tv.tv_sec);
	strftime(tmbuf, sizeof tmbuf, "%m:%d:%H:%M:%S:", nowtm);
	long ms = tv.tv_usec / 1000;
	sprintf(tmbuf2, "%03ld", ms);

	string ret;
	ret += tmbuf;
	ret += tmbuf2;
	return ret;
}

/**
 * A BlockKey which is a hashable key for a specific block.
 * Used as a key in the cache's map, and as a value in the cache's queue.
 * Contains a filename and the position within the file.
 */
struct BlockKey
{
	string fn;
	size_t pos;

	BlockKey(string name, size_t position) : fn(name), pos(position){}

	/**
	 * Returns the BlockKey's representation, for caching purposes.
	 */
	string operator()() const
	{
		stringstream sstm;
		sstm << fn << "#" << pos;
		return sstm.str();
	}

	/**
	 * Returns true iff this block key is smaller that the other one.
	 * (Older is smaller)
	 */
	bool operator<(const BlockKey& rhs) const;

	/**
	 * Returns true iff both BlockKeys refer to the same block.
	 */
	bool operator==(const BlockKey& rhs) const
	{
		return (*this)() == rhs();
	}

	/**
	 * Sets the BlockKey's filename.
	 */
	void set_filename(string name)
	{
		fn = name;
	}

	/**
	 * Returns the BlockKey's string representation.
	 */
	string get_string() const;
};

// Hashing for BlockKey
namespace std
{
	template <>
	struct hash<BlockKey>
	{
		size_t operator()(const BlockKey& bk) const
		{
			return hash<string>()(bk());
		}
	};
}

/**
 * A Block object.
 * Contains the actual data and its size, and the time
 * it was last accessed.
 */
struct Block
{
	char* data;
	size_t size;
	timeval time;

	Block(char* data, size_t size)
	{
		this->data = data;
		this->size = size;
		gettimeofday(&this->time, NULL);
	}

	/**
	 * Returns the Block's string representation.
	 */
	string get_string() const;
};

// The struct for Fuse's private data.
struct caching_state
{
	unordered_map<BlockKey, Block> cache;
	set<BlockKey> queue;
	size_t n_blocks;
	size_t block_size;
	string rootdir;
	FILE* logfile;
};

string BlockKey::get_string() const
{
	return fn.substr(1) + "\t" + to_string( (this->pos / CACHING_DATA->block_size) + 1);
}

string Block::get_string() const
{
	return timeval_to_str(time);
}

bool BlockKey::operator<(const BlockKey& rhs) const
{
	unordered_map<BlockKey, Block>::iterator it1, it2;
	it1 = CACHING_DATA->cache.find(*this);
	it2 = CACHING_DATA->cache.find(rhs);
	timeval t1 = it1->second.time;
	timeval t2 = it2->second.time;
	return timercmp(&t1, &t2, <);
}

// Define the types
typedef struct BlockKey BlockKey;
typedef struct Block Block;
typedef struct caching_state caching_state;

/**
 * prints a message to cout.
 */
void debug(string msg)
{
	cout << msg << endl;
}

/**
 * Outputs an error to cerr when failing to write to the log file
 */
void error_write_log()
{
	cerr << ERROR_WRITE_LOG << endl;
}

/**
 * Logs given message into the log file.
 */
void log_msg(string msg)
{
	if (fprintf(CACHING_DATA->logfile, "%s\n", msg.c_str()) < 0)
	{
		error_write_log();
	}
	fflush(CACHING_DATA->logfile);
}

/**
 * Outputs error message to the log file, and returns the needed errno
 */
int error_system(string func_name)
{
	int ret = -errno;
	string error_str = ERROR_SYSTEM + " " + func_name;
	log_msg(error_str);
	return ret;
}

/**
 * Outputs a usage message to cout when user calls the program with wrong parameters.
 */
void usage()
{
	cout << ERROR_USAGE << endl;
}

/**
 * Outputs an error to cerr when failing to open the log file
 */
void error_open_log()
{
	cerr << ERROR_OPEN_LOG << endl;
}

// Fuse operations struct
struct fuse_operations caching_oper;

/**
 * Returns the full path of the filename src in rootdir.
 * Long paths will break.
 */
static string caching_fullpath(string src)
{
	string res = CACHING_DATA->rootdir;
	res += src;
	if (res.length() > PATH_MAX)
	{
		res.resize(PATH_MAX);
	}
	return res;
}

/**
 * Returns true iff given path is a directory (relative to mountdir)
 */
static bool isdir(const string path)
{
	DIR* dir = opendir(path.c_str());
	if (dir)
	{
	    // Directory exists.
	    closedir(dir);
	    return true;
	}
	// Directory does not exist or can't be opened
	return false;
}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int caching_getattr(const char *path, struct stat *statbuf)
{
    int retstat = 0;

    string fpath = caching_fullpath(path);

    retstat = lstat(fpath.c_str(), statbuf);
    if (retstat != 0)
    {
		retstat = error_system("caching_getattr");
    }
    return retstat;
}

/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int caching_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
	int retstat = 0;
    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
    {
    	retstat = error_system("caching_fgetattr");
    }
    return retstat;
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int caching_access(const char *path, int mask)
{
    int retstat = 0;
    string fpath = caching_fullpath(path);

    retstat = access(fpath.c_str(), mask);

    if (retstat < 0)
    {
		retstat = error_system("caching_access");
    }
    return retstat;
}


/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 */
int caching_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;
    string fpath = caching_fullpath(path);

    fd = open(fpath.c_str(), fi->flags);
    if (fd < 0)
    {
		retstat = error_system("caching_open");
    }
    fi->fh = fd;
    return retstat;
}

/**
 * Inserts the given BlockKey and associated Block to the cache.
 * Assumes block's time is correct.
 */
void insert_block(const BlockKey& bk, const Block& b)
{
	CACHING_DATA->cache.emplace(bk, b);
	CACHING_DATA->queue.insert(bk);
}

/**
 * Erases the block specified bk from the cache.
 */
void erase_block(BlockKey bk)
{
	CACHING_DATA->queue.erase(bk);
	CACHING_DATA->cache.erase(bk);
}

/**
 * Creates new BlockKey and Block fron the given path, position, data pointer and size.
 * Then adds them to the cache (set + map)
 */
void add_to_cache(string path, size_t pos, char* data_ptr, size_t data_size)
{
	BlockKey bk = BlockKey(path, pos);
	Block b = Block((char*) data_ptr, data_size);

	if(CACHING_DATA->cache.size() < CACHING_DATA->n_blocks)
	{
		insert_block(bk, b);
	}
	else
	{
		// Erase oldest block
		BlockKey erasee = *CACHING_DATA->queue.begin();
		unordered_map<BlockKey,Block>::iterator it = CACHING_DATA->cache.find(erasee);
		if (it != CACHING_DATA->cache.end())
		{
			free(it->second.data);
		}
		erase_block(erasee);
		// Add the new one
		insert_block(bk, b);
	}
}

/**
 * returns the filesize of the specified filename.
 */
ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, ifstream::in | ifstream::binary);
    in.seekg(0, ifstream::end);
    return in.tellg();
}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
int caching_read(const char *path, char *buf, size_t size, off_t offset,
				 struct fuse_file_info *fi)
{
	size_t block_size = CACHING_DATA->block_size;
	size_t u_offset = offset;

	// Fix situations where requested size throws us after file end
	string fpath = caching_fullpath(path);
	unsigned int fsize = filesize(fpath.c_str());
	if (u_offset + size > fsize)
	{
		size = fsize - u_offset;
	}

	// Start of first block (relative to file)
	size_t start_pos = u_offset - (u_offset % block_size);

	// Start of last block (relative to file)
	size_t end_pos = u_offset + size - ( (u_offset + size) % block_size);

	char* buff_idx = buf;

	while (start_pos <= end_pos)
	{
		BlockKey bk = BlockKey(path, start_pos);
		unordered_map<BlockKey, Block>::iterator it = CACHING_DATA->cache.find(bk);

		if (it != CACHING_DATA->cache.end())
		{
			// Block exists in cache.
			// return its data, and update the timestamp.
			Block& b = it->second;
			block_size = b.size;

			// Find the relevant index range inside the current block.
			// This is mostly for handling the first and last block.
			size_t block_start_pos = (u_offset > start_pos) ? (u_offset - start_pos) : 0;
			size_t block_end_pos = min<size_t>(block_size, offset + size - start_pos);
			size_t data_size = block_end_pos - block_start_pos;

			if (!memcpy(buff_idx, b.data + block_start_pos, data_size))
			{
				error_system("caching_read");
				return CODE_FAIL;
			}

			buff_idx += data_size;

			// Update block time
			if(gettimeofday(&b.time, NULL) < 0)
			{
				error_system("caching_read");
			}

			start_pos += block_size;

			// Update the queue
			CACHING_DATA->queue.erase(bk);
			CACHING_DATA->queue.insert(bk);
		}

		else
		{
			// Read from disk, add to cache
			size_t block_start_pos = (u_offset > start_pos) ? (u_offset - start_pos) : 0;
			size_t block_end_pos = min<size_t>(block_size, offset + size - start_pos);
			size_t data_size = block_end_pos - block_start_pos;

			char* data_ptr;
			if(posix_memalign((void**)&data_ptr, ALLIGN_OFFSET, block_size) != CODE_SUCCESS)
			{
				error_system("caching_read");
				return CODE_FAIL;
			}

			size_t read_size = pread(fi->fh, data_ptr, block_size, start_pos);
			if (read_size < 0)
			{
				error_system("caching_read");
				free(data_ptr);
				return CODE_FAIL;
			}

			add_to_cache(path, start_pos, data_ptr, read_size);

			memcpy(buff_idx, data_ptr + block_start_pos, data_size);

			buff_idx += data_size;
			start_pos += block_size;
		}
	}

	return buff_idx - buf;
}


/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int caching_flush(const char *path, struct fuse_file_info *fi)
{
	return 0;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int caching_release(const char *path, struct fuse_file_info *fi)
{
	return close(fi->fh);
}

/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int caching_opendir(const char *path, struct fuse_file_info *fi)
{
    DIR *dp;
    int retstat = 0;
    string fpath = caching_fullpath(path);

    dp = opendir(fpath.c_str());
    if (!dp)
    {
		retstat = error_system("caching_opendir");
    }

    fi->fh = (intptr_t) dp;

    return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int caching_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
					struct fuse_file_info *fi)
{
    int retstat = 0;
    DIR *dp;
    struct dirent *de;

    dp = (DIR *) (uintptr_t) fi->fh;

    // Every directory contains at least two entries: . and ..  If my
    // first call to the system readdir() returns NULL I've got an
    // error; near as I can tell, that's the only condition under
    // which I can get an error from readdir()
    de = readdir(dp);
    if (de == 0)
    {
    	return error_system("caching_readdir");
    }

    // This will copy the entire directory into the buffer.  The loop exits
    // when either the system readdir() returns NULL, or filler()
    // returns something non-zero.  The first case just means I've
    // read the whole directory; the second means the buffer is full.
    do
    {
    	if (filler(buf, de->d_name, NULL, 0) != 0)
    	{
    		return -ENOMEM;
    	}
    }
    while ((de = readdir(dp)) != NULL);

    return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int caching_releasedir(const char *path, struct fuse_file_info *fi)
{
	int retstat = closedir((DIR *) (uintptr_t) fi->fh);
	if(retstat != CODE_SUCCESS)
	{
		return error_system("caching_releasedir");
	}
    return retstat;
}

/** Rename a file or directory*/
int caching_rename(const char *path, const char *newpath)
{
	string fpath = caching_fullpath(path);
	string fnewpath = caching_fullpath(newpath);

	bool is_dir = isdir(fpath);
	string path_dir = path;
	if (is_dir)
	{
		path_dir += "/";
	}

	int retstat = rename(fpath.c_str(), fnewpath.c_str());

	if (retstat < 0)
	{
		return error_system("caching_rename");
	}


	unordered_map<BlockKey, Block> blocks;

	unordered_map<BlockKey, Block>::iterator it;
	it = CACHING_DATA->cache.begin();

	// First, save the old blocks into a temp map.
	for(;it != CACHING_DATA->cache.end(); ++it)
	{

		BlockKey bk = it->first;

		if (is_dir && bk.fn.substr(0, path_dir.length()) == path_dir)
		{
			// file is in the requested directory
			Block b = it->second;
			blocks.emplace(bk, b);
		}

		else if (bk.fn == path_dir)
		{
			Block b = it->second;
			blocks.emplace(bk, b);
		}
	}

	string newpath2 = newpath;

	// Now erase from the cache, and insert back after filename change
	for(it = blocks.begin(); it != blocks.end(); ++it)
	{
		BlockKey bk = it->first;
		Block b = it->second;
		erase_block(bk);

		if (is_dir)
		{
			// Change file's prefix to match directory
			string newpath_dir = newpath2;
			newpath_dir += "/" + bk.fn.substr(path_dir.length());
			newpath = newpath_dir.c_str();
		}
		bk.set_filename(newpath);
		insert_block(bk, b);
	}

	return retstat;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
void *caching_init(struct fuse_conn_info *conn)
{
	return CACHING_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void caching_destroy(void *userdata)
{
	unordered_map<BlockKey, Block>::iterator it;

	// Delete block data
	for(it = CACHING_DATA->cache.begin(); it != CACHING_DATA->cache.end(); ++it)
	{
		free(it->second.data);
	}

	// Close log file
	fclose(CACHING_DATA->logfile);

	// Delete private data
	delete CACHING_DATA;
}

/**
 * Ioctl
 *
 * flags will have FUSE_IOCTL_COMPAT set for 32bit ioctls in
 * 64bit environment.  The size and direction of data is
 * determined by _IOC_*() decoding of cmd.  For _IOC_NONE,
 * data will be NULL, for _IOC_WRITE data is out area, for
 * _IOC_READ in area and if both are set in/out area.  In all
 * non-NULL cases, the area is of _IOC_SIZE(cmd) bytes.
 *
 * Introduced in version 2.8
 */
int caching_ioctl(const char *, int cmd, void *arg, struct fuse_file_info *,
				  unsigned int flags, void *data)
{
	timeval now;

	// Print the current time
	if(gettimeofday(&now, NULL) < 0)
	{
		return error_system("caching_ioctl");
	}
	log_msg(timeval_to_str(now));

	// If there are blocks in the cache, print them
	if (CACHING_DATA->queue.size() > 0)
	{
		set<BlockKey>::reverse_iterator rit = CACHING_DATA->queue.rbegin();
		for(; rit != CACHING_DATA->queue.rend(); ++rit)
		{
			BlockKey bk = *rit;
			Block b = CACHING_DATA->cache.find(*rit)->second;
			string line = bk.get_string() + "\t" + b.get_string();
			log_msg(line);
		}
	}
	return CODE_SUCCESS;
}



int main(int argc, char* argv[])
{
	// Initialise the operations
	caching_oper.getattr = caching_getattr;
	caching_oper.access = caching_access;
	caching_oper.open = caching_open;
	caching_oper.read = caching_read;
	caching_oper.flush = caching_flush;
	caching_oper.release = caching_release;
	caching_oper.opendir = caching_opendir;
	caching_oper.readdir = caching_readdir;
	caching_oper.releasedir = caching_releasedir;
	caching_oper.rename = caching_rename;
	caching_oper.init = caching_init;
	caching_oper.destroy = caching_destroy;
	caching_oper.ioctl = caching_ioctl;
	caching_oper.fgetattr = caching_fgetattr;


	caching_oper.readlink = NULL;
	caching_oper.getdir = NULL;
	caching_oper.mknod = NULL;
	caching_oper.mkdir = NULL;
	caching_oper.unlink = NULL;
	caching_oper.rmdir = NULL;
	caching_oper.symlink = NULL;
	caching_oper.link = NULL;
	caching_oper.chmod = NULL;
	caching_oper.chown = NULL;
	caching_oper.truncate = NULL;
	caching_oper.utime = NULL;
	caching_oper.write = NULL;
	caching_oper.statfs = NULL;
	caching_oper.fsync = NULL;
	caching_oper.setxattr = NULL;
	caching_oper.getxattr = NULL;
	caching_oper.listxattr = NULL;
	caching_oper.removexattr = NULL;
	caching_oper.fsyncdir = NULL;
	caching_oper.create = NULL;
	caching_oper.ftruncate = NULL;

	// Check number of arguments
	if (argc < 5)
	{
		usage();
		return CODE_FAIL;
	}

	// Open the log file
	FILE* logfile;
	logfile = fopen(LOG_FILE_NAME, "ab");
	if (logfile == NULL)
	{
		error_open_log();
		return CODE_FAIL;
	}

	// Prepare the private_data
	caching_state* caching_data = new caching_state();
	if (caching_data == NULL) {
		fprintf(logfile, "%s %s\n", ERROR_SYSTEM.c_str(), "main");
		return CODE_FAIL;
	}

	// Add needed info to the private_data
	caching_data->logfile = logfile;
	caching_data->rootdir = argv[1];
	char* mountdir = argv[2];
	caching_data->block_size = atoi(argv[4]);
	caching_data->n_blocks = atoi(argv[3]);

	// Check that input args make sense.
	// Note: if atoi fails (e.g given number of blocks is not a number) it will return 0,
	// So this handles that situation as well.
	if (caching_data->n_blocks <= 0 || caching_data->block_size <= 0
		|| !isdir(caching_data->rootdir) || !isdir(mountdir))
	{
		usage();
		return CODE_FAIL;
	}

	// Prepare argc, argv for fuse's main, include input flags.
	// Include -s flag by default, to avoid multithreading issues.
	int i;
	for(i = 5; i < argc; ++i)
	{
		argv[i-4] = argv[i];
	}
	argv[i-4] = (char*) "-s";
	argv[i-3] = mountdir;
	argc -= 2;

	// Start fuse
	int fuse_stat = fuse_main(argc, argv, &caching_oper, caching_data);
	return fuse_stat;
}
