

// This relates to burstcoin and BHD, which mine by demonstrating storage capacity.
// The network capacity is many orders of magnitude larger than petabytes.
// Meanwhile, other networks like siacoin, have excess capacity.
// Since cryptocurrency is inhibited, for some time there is a space where the capcaity
// of one netowrk could be used to make money on another.
// There are many similar spaces.  This one is near.

#define _FILE_OFFSET_BITS 64
#include <cstdint>

struct nonce
{
	struct scoop {
		uint8_t hash_a[32];
		// in POC2, hash_b is swapped with that of the mirror scoop index2=4095-index1
		uint8_t hash_b[32];
	};
	struct scoop scoops[4096];
};

// each value is a 64-bit number, _not_ zero-prefixed
// POC1 filename: AccountID_StartingNonce_NrOfNonces_Stagger [stagger is nonces per group]
// POC2 filename: AccountID_StartingNonce_NrOfNonces
// 	the data is just a sequence of scoops: all the scoop 0s in nonce order,
// 	  then all scoop 1s in nonce order, etc
//
// For sequential reading, the most optimal way to store a plot file is with an entire
// drive as a single file.

extern "C" {

// from libShabal

/// Creates a single PoC Nonce.
///
/// `plot_buffer` must be correct size - no size checks are performed.
void create_plot(uint64_t account_id,
                 uint64_t nonce,
                 uint8_t poc_version,
                 uint8_t *plot_buffer,
                 uintptr_t plot_buffer_offset);

}

#include <string>

void write_plotfile(uint64_t acct, uint64_t start, uint64_t count, std::string * filename = 0)
{
	uint64_t size = count * sizeof(nonce);
}

uint64_t write_plotfile_bybytes(uint64_t acct, uint64_t start, uint64_t size, std::string * filename = 0)
{
	uint64_t count = size / sizeof(nonce);
	write_plotfile(acct, start, count, filename);
	return start + count;
}

#include <siaskynet_multiportal.hpp>
#include <cassert>

class Generator
{
public:
	static Generator * single;
	Generator(std::string url)
	{
		assert(single == 0);
		single = this;

	}

	std::vector<uint8_t> get(std::string url)
	{
		auto xfer = portal.begin_transfer(sia::skynet_multiportal::transfer_kind::download);
		std::vector<uint8_t> result;
		try {
			sia::skynet portal(xfer.portal);
			result = portal.download(url, {}, std::chrono::milliseconds(1000)).data;
		} catch(...) {
			portal.end_transfer(xfer, 0);
			throw;
		}
		portal.end_transfer(xfer, result.size());
		return result;
	}

	std::string put(std::vector<uint8_t> const & data)
	{
		auto xfer = portal.begin_transfer(sia::skynet_multiportal::transfer_kind::upload);
		std::string result;
		try {
			sia::skynet portal(xfer.portal);
			result = portal.upload({"",data,""}, std::chrono::milliseconds(120000));
		} catch(...) {
			portal.end_transfer(xfer, 0);
			throw;
		}
		portal.end_transfer(xfer, data.size());
		return result;
	}

	struct endpoint
	{
		uint64_t account;
		uint64_t depth;
	} spot;

	sia::skynet_multiportal portal;
};
Generator * Generator::single = 0;

#include "Fusepp/Fuse.cpp"


class PlotFS : public Fusepp::Fuse<PlotFS>
{
public:
	static int getattr(const char *, struct stat *, struct fuse_file_info *)
	{
		int res = 0;
	
		memset(stbuf, 0, sizeof(struct stat));
		if (path == "/") {
			stbuf->st_mode = S_IFDIR | 0755;
			stbuf->st_nlink = 2;
		} else if (path == hello_path) {
			stbuf->st_mode = S_IFREG | 0444;
			stbuf->st_nlink = 1;
			stbuf->st_size = hello_str.length();
		} else
			res = -ENOENT;
	
		return res;
	}
	static int readdir(const char *path, void*buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi, enum fuse_readdir_flags)
	{
		if (path != "/")
			return -ENOENT;
	
		filler(buf, ".", NULL, 0, FUSE_FILL_DIR_PLUS);
		filler(buf, "..", NULL, 0, FUSE_FILL_DIR_PLUS);
		filler(buf, hello_path.c_str() + 1, NULL, 0, FUSE_FILL_DIR_PLUS);
	
		return 0;
	}
	static int open(const char *path, struct fuse_file_info *fi)
	{
		if (path != hello_path)
			return -ENOENT;
	
		if ((fi->flags & 3) != O_RDONLY)
			return -EACCES;
	
		return 0;
	}
	static int read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
	{
		if (path != hello_path)
			return -ENOENT;
	
		size_t len;
		len = hello_str.length();
		if ((size_t)offset < len) {
			if (offset + size > len)
				size = len - offset;
			memcpy(buf, hello_str.c_str() + offset, size);
		} else
			size = 0;
	
		return size;
	}
}

int main(int argc, char **argv)
{
	Generator generator(argv[0]);
	plotfs.run(argc, argv+1);
}
