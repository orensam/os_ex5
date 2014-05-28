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
#include "string"
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

static const int CODE_FAIL = -1;
static const int CODE_SUCCESS = 0;
static const int ROOTDIR_ARG_POS = 2;
static const string USAGE_MSG = "usage: MyCachingFileSystem rootdir mountdir numberOfBlocks blockSize\n";
static const string ERROR_SYSTEM = "system error\n";
static const string ERROR_LOG = "system error: couldn't open ioctloutput.log file\n";

#define LOG_FILE_NAME "ioctloutput.log"


#define CACHING_DATA ((struct caching_state *) fuse_get_context()->private_data)

struct BlockKey
{
	BlockKey(string name, size_t position)
	{
		fn = name;
		pos = position;
	}
	string fn;
	size_t pos;

	string operator()() const
	{
		stringstream sstm;
		sstm << fn << "#" << pos;
		return sstm.str();
	}

	bool operator==(const BlockKey& rhs) const
	{
		return (*this)() == rhs();
	}
};

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

struct Block
{
	Block(char* data, size_t size)
	{
		this->data = data;
		this->size = size;
	}
	char* data;
	size_t size;
	timeval* time;
};

struct BlockKeyPtr
{
	BlockKey* ptr;
	bool operator<(const BlockKeyPtr& rhs) const;
	bool operator==(const BlockKeyPtr& rhs) const;
};


struct caching_state
{
	unordered_map<BlockKey, Block> cache;
	set<BlockKeyPtr> queue;
	int n_blocks;
	int block_size;
	string rootdir;
	FILE* logfile;
};

bool BlockKeyPtr::operator<(const BlockKeyPtr& rhs) const
{
	timeval* t1 = (CACHING_DATA->cache[*ptr]).time;
	timeval* t2 = (CACHING_DATA->cache[*rhs.ptr]).time;
	return timercmp(t1, t2, <);
}

bool BlockKeyPtr::operator==(const BlockKeyPtr& rhs) const
{
	return (*(this->ptr) == *(rhs.ptr));
}

typedef struct BlockKeyPtr BlockKeyPtr;
typedef struct BlockKey BlockKey;
typedef struct Block Block;
typedef struct caching_state caching_state;


void usage()
{
	cerr << USAGE_MSG << endl;
}

void error_system()
{
	// TODO: print to log, not stderr
	fprintf(CACHING_DATA->logfile, ERROR_SYSTEM.c_str());
}

void debug(string msg)
{
	msg += '\n';
	fprintf(CACHING_DATA->logfile, msg.c_str());
}

void error_log()
{
	cerr << ERROR_LOG << endl;
}

struct fuse_operations caching_oper;

static string caching_fullpath(string src)
{
	string res = CACHING_DATA->rootdir;
	res += src;

	debug("in caching_fullpath. res is:");
	debug(res);

	return res;

}

/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int caching_getattr(const char *path, struct stat *statbuf)
{
	debug("in getattr");
    int retstat = 0;
    string fpath = caching_fullpath(path);
    retstat = lstat(fpath.c_str(), statbuf);
    if (retstat != 0)
    {
		error_system();
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
	debug("in fgetattr");
	int retstat = 0;
    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
    {
    	error_system();
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
		error_system();
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
int caching_open(const char *path, struct fuse_file_info *fi){
    int retstat = 0;
    int fd;
    string fpath = caching_fullpath(path);

    fd = open(fpath.c_str(), fi->flags|O_DIRECT|O_SYNC);
    if (fd < 0)
    {
		error_system();
    }
    fi->fh = fd;

    //TODO: check this:
//    log_fi(fi);

    return retstat;
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
	int retstat;

//	int block_size = CACHING_DATA->block_size;

	int start_pos = offset - (offset % block_size);
	int end_pos = offset + size - ( (offset + size) % block_size);

	while (start_pos <= end_pos)
	{
		int buff_idx = 0;
		BlockKey bk = BlockKey{path, start_pos};
		unordered_map<BlockKey, Block>::iterator it = CACHING_DATA->cache.find(bk);
		if (it != CACHING_DATA->cache.end())
		{
			// Block exists in cache.
			// return its data, and update the timestamp.

			Block& b = it->second;
			int block_size = b.size;

			int block_start_pos = max(0, (int) offset - start_pos);
			int block_end_pos = min(block_size, end_pos + block_size - ((int) offset + (int) size));
			int data_size = block_end_pos - block_start_pos;

			memcpy(buf + buff_idx, b.data + block_start_pos, data_size);

			gettimeofday(b.time, NULL);

			// Update the queue
			CACHING_DATA->queue.erase(BlockKeyPtr(&bk));
			CACHING_DATA->queue.insert(BlockKeyPtr(&bk));
		}
		else
		{
			// Read from disk, add to cache


		}
	}


	//
	retstat = pread(fi->fh, buf, size, offset);
	//
	return retstat;
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
int caching_release(const char *path, struct fuse_file_info *fi){
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

    debug("in opendir. trying to read dir: ");
    debug(fpath);
    dp = opendir(fpath.c_str());
    if (!dp)
    {
		error_system();
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

    debug("in readdir");
    de = readdir(dp);
    if (de == 0)
    {
    	error_system();
    	return retstat;
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
int caching_releasedir(const char *path, struct fuse_file_info *fi){

	int retstat = 0;
    closedir((DIR *) (uintptr_t) fi->fh);
    return retstat;
}

void insert_block(string fn, size_t pos, Block& b)
{
	BlockKey bk = BlockKey(fn, pos);
	CACHING_DATA->cache.emplace(bk, b);
	CACHING_DATA->queue.insert(BlockKeyPtr(bk));
}

void erase_block(BlockKey bk)
{
	CACHING_DATA->cache.erase(bk);
	CACHING_DATA->queue.erase(BlockKeyPtr(bk));
}

/** Rename a file */
int caching_rename(const char *path, const char *newpath)
{
	string fpath = caching_fullpath(path);
	string fnewpath = caching_fullpath(newpath);

	int retstat = rename(fpath.c_str(), fnewpath.c_str());

	if (retstat < 0)
	{
		error_system();
	}
	else
	{
		unordered_map<BlockKey, Block>::iterator it = CACHING_DATA->cache.begin();
		while(it != CACHING_DATA->cache.end())
		{
			BlockKey& bk = it->first;
			if (bk.fn == path)
			{
				Block b = it->second;
				erase_block(bk);
				insert_block(path, bk.pos, b);
			}
		}
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
int caching_ioctl (const char *, int cmd, void *arg, struct fuse_file_info *,
				   unsigned int flags, void *data)
{
	return 0;
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
//	caching_oper.rename = NULL;
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

	if (argc < 5)
	{
		usage();
		return CODE_FAIL;
	}

	caching_state* caching_data = new caching_state();
	if (caching_data == NULL) {
		error_system();
		return CODE_FAIL;
	}


	caching_data->rootdir = argv[1];

//	char* mountdir = argv[2];
//
//	char* argv2[4];
//	argv2[0] = argv[0];
//	argv2[1] = (char*) "-f";
//	argv2[2] = (char*) "-s";
//	argv2[3] = mountdir;
//
//	argc = 4;

//	argv[1] = argv[2];
	for (int i = 3; i< (argc - 1); i++){
		argv[i] = NULL;
	}
	argv[1] = (char*) "-s";
	argc = 3;

	FILE* logfile;
	logfile = fopen(LOG_FILE_NAME, "a");
	caching_data->logfile = logfile;

	int fuse_stat = fuse_main(argc, argv, &caching_oper, caching_data);
	return fuse_stat;
}

















