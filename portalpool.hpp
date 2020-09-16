#include <siaskynet_multiportal.hpp>

#include <thread>

// For outputting a message on stderr when a portal fails
#include <iostream>

namespace sia {

class portalpool {
public:
	portalpool(double bytes_bandwidth_down = 1024, double bytes_bandwidth_up = 1024, size_t connections_down = 8, size_t connections_up = 4)
	: bandwidth{bytes_bandwidth_down / connections_down, bytes_bandwidth_up / connections_up}
	{
		for (size_t i = 0; i < connections_down; ++ i) {
			workers[skynet_multiportal::download].emplace_back(worker{i, std::unique_ptr<skynet>(new skynet())});
			free[skynet_multiportal::download].push_back(i);
		}
		for (size_t i = 0; i < connections_up; ++ i) {
			workers[skynet_multiportal::upload].emplace_back(worker{i, std::unique_ptr<skynet>(new skynet())});
			free[skynet_multiportal::upload].push_back(i);
		}
	}

	struct worker {
		size_t index;
		std::unique_ptr<skynet> portal;
		skynet_multiportal::transfer transfer;
	};

	worker const * takeworkerout(skynet_multiportal::transfer_kind kind, bool block = true)
	{
		std::unique_lock<std::mutex> lock(worker_lists);
		while (!free[kind].size()) {
			if (!block) { return 0; }
			worker_free.wait(lock);
		}
		worker * w = &workers[kind][free[kind].back()];
		free[kind].pop_back();
		return w;
	}

	void workstart(worker const * w, skynet_multiportal::transfer_kind kind)
	{
		const_cast<worker *>(w)->transfer = multiportal.begin_transfer(kind);
		const_cast<worker *>(w)->portal->options = w->transfer.portal;
	}

	void workstop(worker const * w, size_t size) {
		multiportal.end_transfer(w->transfer, size);
	}

	void putworkerback(worker const * w) {
		{
			std::unique_lock<std::mutex> lock(worker_lists);
			free[w->transfer.kind].push_back(w->index);
		}
		worker_free.notify_all();
	}

	skynet::response download(std::string const & skylink, std::initializer_list<std::pair<size_t, size_t>> ranges = {}, size_t maxsize = 1024*1024*64, bool fail = false, worker const * w = 0)
	{
		auto timeout = std::chrono::milliseconds((unsigned long)(1000 * maxsize / bandwidth[skynet_multiportal::download]));

		auto worker = w;
		skynet::response result;
		if (w == 0) {
			worker = takeworkerout(skynet_multiportal::download);
		}
		while ("retrying download") {
			try {
				workstart(worker, skynet_multiportal::download);
				result = worker->portal->download(skylink, ranges, timeout);
				workstop(worker, result.data.size() + result.filename.size());
				break;
			} catch(std::runtime_error const & e) {
				workstop(worker, 0);
				std::cerr << w->portal->options.url << ": " << e.what() << std::endl;
				if (fail) {
					result = {};
					break;
				}
			}
		}
		if (w == 0) {
			putworkerback(worker);
		}
		return result;
	}

	std::string upload(std::string const & filename, std::vector<skynet::upload_data> const & files, bool fail = false, worker const * w = 0)
	{
		auto worker = w;
		size_t size = 0;
		for (auto & file : files) {
			size += file.data.size() + file.filename.size() + file.contenttype.size();
		}
		auto timeout = std::chrono::milliseconds((unsigned long)(1000 * size / bandwidth[skynet_multiportal::upload]));
		
		std::string link;
		if (w == 0) {
			worker = takeworkerout(skynet_multiportal::upload);
		}
		while ("retrying upload") {
			try {
				workstart(worker, skynet_multiportal::upload);
				link = worker->portal->upload(filename, files, timeout);
				workstop(worker, size);
				break;
			} catch(std::runtime_error const & e) {
				workstop(worker, 0);
				std::cerr << w->portal->options.url << ": " << e.what() << std::endl;
				if (fail) {
					link = {};
					break;
				}
			}
		}
		if (w == 0) {
			putworkerback(worker);
		}
		return link;
	}

	std::mutex worker_lists;
	std::condition_variable worker_free;

	size_t available_down()
	{
		std::unique_lock<std::mutex> lock(worker_lists);
		return free[skynet_multiportal::download].size();
	}

	size_t available_up()
	{
		std::unique_lock<std::mutex> lock(worker_lists);
		return free[skynet_multiportal::upload].size();
	}
	
private:
	double bandwidth[2];
	sia::skynet_multiportal multiportal;
	
	std::vector<worker> workers[2];
	std::vector<size_t> free[2];
};

}
