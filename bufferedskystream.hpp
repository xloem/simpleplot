#include <thread>
#include <map>
#include <unordered_set>

#include "skystream.hpp"

class bufferedskystream : public skystream
{
public:
	using skystream::skystream;
	bufferedskystream(sia::portalpool & portalpool, std::mutex & groupmutex, std::condition_variable & reading, size_t maxblocksize = 1024*1024*128)
	: skystream(portalpool),
	  groupmutex(groupmutex),
	  reading(reading),
	  maxblocksize(maxblocksize)
	{
		start();
	}
	bufferedskystream(nlohmann::json identifiers, sia::portalpool & portalpool, std::mutex & groupmutex, std::condition_variable & reading, size_t maxblocksize = 1024*1024*128)
	: skystream(identifiers, portalpool),
	  groupmutex(groupmutex),
	  reading(reading),
	  maxblocksize(maxblocksize)
	{
		start();
	}

	bufferedskystream(bufferedskystream const &) = default;
	bufferedskystream(bufferedskystream &&) = default;

	~bufferedskystream()
	{
		if (backlogup()) {
			std::cerr << "not flushed" << std::endl;
			exit(-1);
		}
	}

	void shutdown()
	{
		{
			std::lock_guard<std::mutex> lock(mutex);
			if (!pumping) {
				return;
			}
			pumping = false;
		}
		reading.notify_all();
	}

	uint64_t sizeup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return offsetup;
	}
	uint64_t backlogup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return queueup.size();
	}
	uint64_t processedup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return offsetup - queueup.size();
	}
	std::pair<uint64_t,uint64_t> processed_and_total()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return {offsetup - queueup.size(), offsetup};
	}
	void basictipmetadata(nlohmann::json & identifiers, uint64_t & uploaded, uint64_t & total)
	{
		std::lock_guard<std::mutex> lock(mutex);
		identifiers = skystream::identifiers();
		total = offsetup;
		uploaded = total - queueup.size();
	}

	void queue_local_up(std::vector<uint8_t> && data)
	{
		size_t uploaded = 0;
		while (uploaded < data.size()) {
			size_t toupload = data.size() - uploaded;
			{
				std::unique_lock<std::mutex> lock(mutex);
				if (maxblocksize > 0) {
					while (pumping && queueup.size() >= maxblocksize*2) {
						this->uploaded.wait(lock);
					}
					if (queueup.size() + toupload > maxblocksize*2) {
						toupload = maxblocksize*2 - queueup.size() ;
					}
				}
				// TODO: don't drop data on shutdown, certainly not mid-request
				if (!pumping) {
					lock.unlock();
					moredataup.notify_all();
					return;
				}
				queueup.insert(queueup.end(), data.begin() + uploaded, data.begin() + uploaded + toupload);
			}
			moredataup.notify_all();
			uploaded += toupload;
		}
	}

	ssize_t queue_net_down()
	{
		// we want to loop over the length, queueing downloaders as they
		// are available.
		size_t offset = 0;
		size_t tail = 0;
		while ("pumploop") {
			{ // get current request
				std::unique_lock<std::mutex> lock(mutex);
				offset = offsetdown;
				tail = taildown;
			}
			{ // check for shutdown
				std::unique_lock<std::mutex> lock(mutex);
				if  (!pumping) {
					lock.unlock();
					moredatadown.notify_all();
					return -1;
				}
			}
			// check if the request has content
			if (offset >= tail) {
				// no: wait for another?
				std::unique_lock<std::mutex> grouplock(groupmutex);
				reading.wait(grouplock);
				continue;
			}
			sia::portalpool::worker const * worker = 0;
			size_t startpos = offset;
			try {
				//std::cerr << "Looking for workers to download " << offset << " to " << tail << std::endl;

				// The first reason this is slow appears to be the time spent downloading the tree in the block_span call.  This call would be avoided by downloading by index instead of by bytes.

				// start by waiting for at least one
				while (0 == (worker = portalpool.takeworkerout(sia::skynet_multiportal::download, false))) {
					std::unique_lock<std::mutex> lock(portalpool.worker_lists);
					portalpool.worker_free.wait(lock);
				}
				auto range = block_span("bytes", offset, worker);
				auto d = new downloader(*this, worker, range.first, range.second);
				{
					std::unique_lock<std::mutex> lock(mutex);
					queuedown[range.first] = std::unique_ptr<downloader>(d);
				}
				worker = 0;
				offset = range.second;
				// then add more if there are free workers
				while ((worker = portalpool.takeworkerout(sia::skynet_multiportal::download, false))) {
					range = block_span("bytes", offset, worker);
					d = new downloader(*this, worker, range.first, range.second);
					{
						std::unique_lock<std::mutex> lock(mutex);
						queuedown[range.first] = std::unique_ptr<downloader>(d);
					}
					worker = 0;
					offset = range.second;
					range = block_span("bytes", offset);
				}
			} catch (std::out_of_range) { } // thrown at end of stream
					// note workers and calls to block_span
					// are ordered so as to workaround not
					// having implemented RAII for struct worker
					// in the face of the out_of_range exception
			{
				std::unique_lock<std::mutex> lock(mutex);
				//std::cerr << "::downloading to " << offset << " with " << queuedown.size() << " workers " << std::endl;
				//std::cerr << "pool now has " << portalpool.available_down() << " available."  << std::endl;
			}
			return offset - startpos;
		}
	}

	std::mutex read_mutex;
	std::vector<uint8_t> xfer_local_down(uint64_t offset, uint64_t eventualtail = 0)
	{
		std::lock_guard<std::mutex> read_lock(read_mutex);
		{
			std::unique_lock<std::mutex> lock(mutex);
			taildown = eventualtail;
			// remove queued items outside expected range
			for (auto it = queuedown.begin(); it != queuedown.end();) {
				// for now this mutex waits for completion
				// ideally there would be a way to cancel skynet txs
				// we could also move it to some other list
				std::unique_lock lock(it->second->mutex);
				if (it->first > taildown || it->first + it->second->data.size() < offset) {
					auto backup = std::move(it->second);
					it = queuedown.erase(it);
					lock.unlock();
				} else {
					++ it;
				}
			}
			auto range = block_span("bytes", offset);
			//std::cerr << "range of block around " << offset << " is [" << range.first << "," << range.second << ")" << std::endl;
			offsetdown = range.first;
		}
		reading.notify_all();
		{
			std::unique_lock<std::mutex> lock(mutex);
			// we now need to wait until the queue contains our block.
			while (pumping && queuedown.count(offsetdown) == 0) {
				//std::cerr << "Waiting for " << offsetdown << " ..." << std::endl;
				moredatadown.wait(lock);
				//std::cerr << "After waiting, item count is " << queuedown.count(offsetdown) << " total size is " << queuedown.size() << std::endl;
			}
			std::vector<uint8_t> result;
			while (queuedown.count(offsetdown)) {
				auto item = std::move(queuedown[offsetdown]);
				queuedown.erase(offsetdown);
				std::scoped_lock(item->mutex);
				result.insert(result.end(), item->data.begin(), item->data.end());
				//std::cerr << "Ferrying " << item->data.size() << " bytes" << std::endl;
				offsetdown += item->data.size();
			}
			return result;
		}
	}

	ssize_t xfer_net_up()
	{
		while ("pumploop") {
			std::vector<uint8_t> data;

			{
				std::unique_lock<std::mutex> lock(mutex);
				while (pumping && queueup.size() == 0) {
					moredataup.wait(lock);
				}
				if (!pumping && queueup.size() == 0) {
					lock.unlock();
					uploaded.notify_all();
					return -1;
				}
				if (maxblocksize <= 0 || queueup.size() <= maxblocksize) {
					data = std::move(queueup);
				} else {
					data.clear();
					data.insert(data.begin(), queueup.begin(), queueup.begin() + maxblocksize);
					queueup.erase(queueup.begin(), queueup.begin() + maxblocksize);
				}
			}
			if (data.size()) {
				write(data, "bytes", offsetup);
				{
					std::lock_guard<std::mutex> lock(mutex);
					offsetup += data.size();
				}
				uploaded.notify_all();
			}
			return data.size();
		}
	}

	std::mutex & groupmutex; 
	std::condition_variable & reading; // notified when read is requested

	std::mutex mutex;
	std::condition_variable uploaded; // notified when write queue is emptied
	std::condition_variable moredataup; // notified when write queue lengthens
	std::condition_variable moredatadown; // notified when read queue lengthens

private:
	friend struct downloader;
	struct downloader
	{
		bufferedskystream & stream;
		std::thread process;
		size_t start;
		size_t tail;
		std::condition_variable downloaded;
		std::vector<uint8_t> data;
		std::mutex mutex;

		downloader(bufferedskystream & stream, sia::portalpool::worker const * worker, size_t node_start, size_t node_end)
		: stream(stream), worker(worker)
		{
			std::unique_lock<std::mutex> lock(stream.mutex);
			start = node_start;
			tail = node_end;
			//std::cerr << "Downloading " << start << " to " << tail << std::endl;
			process = std::thread(&downloader::download, this, std::move(std::unique_lock(mutex)));
		}
		~downloader()
		{
			process.join();
		}
	private:
		void download(std::unique_lock<std::mutex> && lock)
		{
			double offset = start;
			data = stream.skystream::read("bytes", offset, "real", worker);
			stream.portalpool.putworkerback(worker);
			worker = 0;
			//std::cerr << "notifying " << start << std::endl;
			lock.unlock();
			downloaded.notify_all();
			stream.moredatadown.notify_all();
		}
		sia::portalpool::worker const * worker;
	};
	void start()
	{
		std::lock_guard<std::mutex> lock(mutex);
		offsetup = span("bytes").second;
		offsetdown = 0;
		taildown = 0;
	}
	bool pumping = true;
	size_t maxblocksize;
	std::map<size_t, std::unique_ptr<downloader>> queuedown;
	std::vector<uint8_t> queueup;
	size_t offsetup;
	size_t offsetdown, taildown;
};
