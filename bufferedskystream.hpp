#include <functional>
#include <thread>
#include <map>
#include <unordered_set>

#include "skystream.hpp"

// we added rading/writing conditions to wait on in net pumps.
// they have a small race problem because a shared variable is not used.
// the nets are the pumps and the locals are the requests

// we want to proces most-needy
// the condition variable is when something becomes needy
// then we have a shared variable for how needy
// protected by a mutex.

// seems time to make a bufferedskystreams class
// so a pointer to it can be passed to bufferedskystream

// runs the net pumps of multiple skystreams together
class bufferedskystream;
class bufferedskystreams
{
friend class bufferedskystream;
public:
	bufferedskystreams(sia::portalpool & portalpool, size_t maxblocksize = 1024*1024*128, std::function<void(bufferedskystream&,uint64_t)> down_callback = {}, std::function<void(bufferedskystream&,uint64_t)> up_callback = {})
	: portalpool(portalpool),
	  maxblocksize(maxblocksize)
	{
		pumping = true;
		down_thread = std::thread(&bufferedskystreams::pump_down, this);
		up_thread = std::thread(&bufferedskystreams::pump_up, this);
	}
	~bufferedskystreams()
	{
		shutdown();
	}
	void shutdown();

	size_t add(nlohmann::json identifiers = {});

	void set_down_callback(std::function<void(bufferedskystream&,uint64_t)> callback)
	{
		std::scoped_lock lock(streams_mutex);
		down_callback = callback;
	}

	void set_up_callback(std::function<void(bufferedskystream&,uint64_t)> callback)
	{
		std::scoped_lock lock(streams_mutex);
		up_callback = callback;
	}

	size_t size()
	{
		std::scoped_lock lock(streams_mutex);
		return streams.size();
	}

	bufferedskystream & get(size_t index)
	{
		std::scoped_lock lock(streams_mutex);
		return *streams[index];
	}

private:
	bool pumping;
	std::mutex streams_mutex;
	std::vector<std::unique_ptr<bufferedskystream>> streams;
	sia::portalpool & portalpool;
	size_t maxblocksize;

	std::condition_variable down_new;
	std::condition_variable up_new;
	std::multimap<uint64_t,bufferedskystream*,std::greater<uint64_t>> down_priorities;
	std::multimap<uint64_t,bufferedskystream*,std::greater<uint64_t>> up_priorities;
	std::mutex down_priorities_mutex;
	std::mutex up_priorities_mutex;

	std::thread down_thread;
	std::thread up_thread;
	std::function<void(bufferedskystream&,uint64_t)> up_callback, down_callback;

	void pump_down();
	void pump_up();
};

class bufferedskystream : public skystream
{
public:
	bufferedskystream(bufferedskystreams & group, size_t index = 0, nlohmann::json identifiers = {})
	: skystream(group.portalpool, identifiers),
	  group(group),
	  _index(index)
	{
		start();
	}

	size_t index()
	{
		return _index;
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
		group.down_new.notify_all();
		group.up_new.notify_all();
	}

	uint64_t sizeup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return tailup;
	}
	uint64_t backlogup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return tailup - offsetup;
	}
	uint64_t processedup()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return offsetup;
	}
	std::pair<uint64_t,uint64_t> processed_and_total()
	{
		std::lock_guard<std::mutex> lock(mutex);
		return {offsetup, tailup};
	}
	void basictipmetadata(nlohmann::json & identifiers, uint64_t & uploaded, uint64_t & total)
	{
		std::lock_guard<std::mutex> lock(mutex);
		identifiers = skystream::identifiers();
		total = tailup;
		uploaded = offsetup;
	}

	void queue_local_up(std::vector<uint8_t> && data)
	{
		size_t uploaded = 0;
		while (uploaded < data.size()) {
			size_t toupload = data.size() - uploaded;
			{
				std::unique_lock lock(group.up_priorities_mutex);
				if (group.maxblocksize > 0) {
					while (queueup.size() >= group.maxblocksize*2) {
						this->uploaded.wait(lock);
					}
					if (queueup.size() + toupload > group.maxblocksize*2) {
						toupload = group.maxblocksize*2 - queueup.size() ;
					}
				}
				{
					std::unique_lock lock(mutex);
					tailup += toupload;
				}
				queueup.insert(queueup.end(), data.begin() + uploaded, data.begin() + uploaded + toupload);
				if (queueup.size() != uppriority) {
					for (auto range = group.up_priorities.equal_range(uppriority); range.first != range.second; ++range.first) {
						if (range.first->second == this) {
							group.up_priorities.erase(range.first);
							break;
						}
					}
					uppriority = queueup.size();
				}
				assert(uppriority);
				auto spot = group.up_priorities.emplace(uppriority, this);
				if (spot == group.up_priorities.begin()) {
					lock.unlock();
					group.up_new.notify_all();
				}
			}
			uploaded += toupload;
		}
	}


	// pumps one transfer cycle for downloads, returns bytes pumped or -1 if shut down
	ssize_t queue_net_down()
	{
		size_t offset = 0;
		size_t tail = 0;
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
			// no
			return 0;
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

	std::mutex read_mutex;
	std::vector<uint8_t> xfer_local_down(uint64_t offset, uint64_t size = 0, int64_t eventualtail = -1)
	{
		if (eventualtail == -1) {
			eventualtail = span("bytes").second;
		}
		if (size == 0) {
			size = eventualtail - offset;
		}
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
		{
			std::unique_lock<std::mutex> lock(mutex);
			// we now need to wait until the queue contains our block.
			while (pumping && queuedown.count(offsetdown) == 0) {
				{
					std::unique_lock lock(group.down_priorities_mutex);
					for(auto range = group.down_priorities.equal_range(downpriority); range.first != range.second; ++range.first) {
						if (range.first->second == this) {
							group.down_priorities.erase(range.first);
							break;
						}
					}
					downpriority = taildown - offsetdown;
					auto spot = group.down_priorities.emplace(downpriority,this);
					if (spot == group.down_priorities.begin()) {
						lock.unlock();
						group.down_new.notify_all();
					}
				}
				moredatadown.wait(lock);
			}
			std::vector<uint8_t> result;
			while (queuedown.count(offsetdown)) {
				auto & itemr = queuedown[offsetdown];
				std::scoped_lock(itemr->mutex);
				if (offset + size < offsetdown + itemr->data.size()) {
					// request ends before block does
					if (offsetdown < offset) {
						result.insert(result.end(), itemr->data.begin() + offset - offsetdown, itemr->data.begin() + offset + size - offsetdown);
					} else {
						result.insert(result.end(), itemr->data.begin(), itemr->data.begin() + offset + size - offsetdown);
					}
					return result;
				}
				auto item = std::move(itemr);
				queuedown.erase(offsetdown);
				auto itembegin = item->data.begin();
				if (offset > offsetdown) {
					itembegin += offset - offsetdown;
				}
				result.insert(result.end(), itembegin, item->data.end());
				//std::cerr << "Ferrying " << (item->data.end() - itembegin) << " bytes" << std::endl;
				offsetdown += item->data.size();
			}
			return result;
		}
	}

	// pump one transfer cycle for uploads, return bytes pumped or -1 if shut down
	ssize_t xfer_net_up()
	{
		std::vector<uint8_t> data;
		size_t offset;
		{
			std::unique_lock<std::mutex> lock(mutex);
			if (!pumping) {
				if (offsetup == tailup) {
					lock.unlock();
					uploaded.notify_all();
					return -1;
				}
			} else if (offsetup == tailup) {
				return 0;
			}
			offset = offsetup;
		}
		// pull data to transfer into local variable
		{
			std::unique_lock lock(group.up_priorities_mutex);
			if (group.maxblocksize <= 0 || queueup.size() <= group.maxblocksize) {
				data = std::move(queueup);
			} else {
				data.insert(data.begin(), queueup.begin(), queueup.begin() + group.maxblocksize);
				queueup.erase(queueup.begin(), queueup.begin() + group.maxblocksize);
			}
		}
		if (data.size()) {
			write(data, "bytes", offset);
			{
				std::lock_guard<std::mutex> lock(mutex);
				offsetup += data.size();
			}
			uploaded.notify_all();
		}
		{
			std::unique_lock lock(group.up_priorities_mutex);
			if (queueup.size() == 0) {
				assert(uppriority != 0); // logic flow with queue_local_up.  i thought it could be good to have only nonzero priorities in the queue, ever.
				for (auto range = group.up_priorities.equal_range(uppriority); range.first != range.second; ++range.first) {
					if (range.first->second == this) {
						group.up_priorities.erase(range.first);
						break;
					}
				}
				uppriority = 0;
			}
		}
		return data.size();
	}

	std::mutex mutex;
	std::condition_variable uploaded; // notified when write queue is emptied
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
		tailup = offsetup;
		offsetdown = 0;
		taildown = 0;
		downpriority = 0;
		uppriority = 0;
	}
	bufferedskystreams & group;
	size_t const _index;
	bool pumping = true;
	std::map<size_t, std::unique_ptr<downloader>> queuedown;
	std::vector<uint8_t> queueup;
	size_t offsetdown, taildown;
	size_t offsetup, tailup;
	uint64_t downpriority;
	uint64_t uppriority;
};


void bufferedskystreams::shutdown()
{
	{
		std::scoped_lock lock(streams_mutex);
		pumping = false;
		for (auto & stream : streams) {
			stream->shutdown();
		}
	}
	down_new.notify_all();
	up_new.notify_all();
	if (down_thread.joinable()) {
		down_thread.join();
	}
	if (up_thread.joinable()) {
		up_thread.join();
	}
}

size_t bufferedskystreams::add(nlohmann::json identifiers)
{
	std::scoped_lock lock(streams_mutex);
	streams.emplace_back(new bufferedskystream(*this, streams.size(), identifiers));
	if (!pumping) { streams.back()->shutdown(); }
	return streams.size() - 1;
}

void bufferedskystreams::pump_down()
{
	bufferedskystream * stream;
	
	while("pumping") {
		{
			std::unique_lock lock(down_priorities_mutex);
			if (down_priorities.size() == 0) {
				{
					std::scoped_lock(streams_mutex);
					if (!pumping) {
						return;
					}
				}
				down_new.wait(lock);
				continue;
			}
			stream = down_priorities.begin()->second;
			// TODO: erase this next line, and have queue_net_down do it like in pump_up
			down_priorities.erase(down_priorities.begin());
		}
		ssize_t size = stream->queue_net_down();
		if (size > 0) {
			if (down_callback) {
				down_callback(*stream, size);
			}
		}
	}
}
void bufferedskystreams::pump_up()
{
	bufferedskystream * stream;
	decltype(up_priorities)::iterator streamit;

	while("pumping") {
		{
			std::unique_lock lock(up_priorities_mutex);
			if (up_priorities.size() == 0) {
				{
					std::scoped_lock(streams_mutex);
					if (!pumping) {
						break;
					}
				}
				up_new.wait(lock);
				continue;
			}
			streamit = up_priorities.begin();
			stream = streamit->second;
		}
		ssize_t size = stream->xfer_net_up(); // empties itself from up_priorities
		if (size > 0) {
			if (up_callback) {
				up_callback(*stream, size);
			}
		}
	}
}
