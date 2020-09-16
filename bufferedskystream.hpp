#include <thread>

#include "skystream.hpp"

class bufferedskystream : public skystream
{
public:
	using skystream::skystream;
	bufferedskystream(sia::portalpool & portalpool, std::mutex & groupmutex, std::condition_variable & reading, size_t maxblocksize = 1024*1024*2)
	: skystream(portalpool),
	  groupmutex(groupmutex),
	  reading(reading),
	  maxblocksize(maxblocksize)
	{
		start();
	}
	bufferedskystream(nlohmann::json identifiers, sia::portalpool & portalpool, std::mutex & groupmutex, std::condition_variable & reading, size_t maxblocksize = 1024*1024*2)
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
		size_t offset = 0;
		size_t tail = 0;
		while ("pumploop") {
			std::vector<uint8_t> data;
			{
				{
					std::unique_lock<std::mutex> lock(mutex);
					offset = offsetdown + queuedown.size();
					tail = taildown;
				}
				if (offset >= tail) {
					std::unique_lock<std::mutex> grouplock(groupmutex);
					reading.wait(grouplock);
				}
				{
					std::unique_lock<std::mutex> lock(mutex);
					if  (!pumping) {
						lock.unlock();
						moredatadown.notify_all();
						return -1;
					}
				}
			}
			if (offset < tail) {
				double passedoffset = offset;
				auto data = skystream::read("bytes", passedoffset);
				size_t size = 0;
				{
					std::lock_guard<std::mutex> lock(mutex);
					if (offsetdown + queuedown.size() == offset) {
						// data still desired
						size = data.size();
						queuedown.insert(queuedown.end(), data.begin(), data.end());
					}
				}
				moredatadown.notify_all();
				return size;
			} 
		}
	}

	std::mutex read_mutex;
	std::vector<uint8_t> xfer_local_down(uint64_t offset, uint64_t eventualtail = 0)
	{
		reading.notify_all();
		std::lock_guard<std::mutex> read_lock(read_mutex);
		std::unique_lock<std::mutex> lock(mutex);
		taildown = eventualtail;
		if (offsetdown != offset) {
			offsetdown = offset;
			queuedown.clear();
		}
		while (pumping && queuedown.size() == 0) {
			moredatadown.wait(lock);
		}
		offsetdown += queuedown.size();
		return std::move(queuedown);
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
	void start()
	{
		std::lock_guard<std::mutex> lock(mutex);
		offsetup = span("bytes").second;
		offsetdown = 0;
		taildown = 0;
	}
	bool pumping = true;
	size_t maxblocksize;
	std::vector<uint8_t> queueup;
	std::vector<uint8_t> queuedown;
	size_t offsetup;
	size_t offsetdown, taildown;
};
