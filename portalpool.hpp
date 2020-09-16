#include <siaskynet_multiportal.hpp>

#include <thread>

// For outputting a message on stderr when a portal fails
#include <iostream>

namespace sia {

class portalpool {
public:
	portalpool(double bytes_bandwidth_down = 1024, double bytes_bandwidth_up = 1024, size_t connections_down = 4, size_t connections_up = 4)
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

	skynet::response download(std::string const & skylink, std::initializer_list<std::pair<size_t, size_t>> ranges = {}, size_t maxsize = 1024*1024*64, bool fail = false)
	{
		auto timeout = std::chrono::milliseconds((unsigned long)(1000 * maxsize / bandwidth[skynet_multiportal::download]));

		while ("retrying download") {
			auto & worker = takeworkerout(skynet_multiportal::download);
			try {
				auto response = worker.portal->download(skylink, ranges, timeout);
				putworkerback(worker, response.data.size() + response.filename.size());
				return response;
			} catch(std::runtime_error const & e) {
				std::cerr << worker.portal->options.url << ": " << e.what() << std::endl;
				putworkerback(worker, 0);
				if (fail) { return {}; }
			}
		}
	}

	std::string upload(std::string const & filename, std::vector<skynet::upload_data> const & files, bool fail = false)
	{
		size_t size = 0;
		for (auto & file : files) {
			size += file.data.size() + file.filename.size() + file.contenttype.size();
		}
		auto timeout = std::chrono::milliseconds((unsigned long)(1000 * size / bandwidth[skynet_multiportal::upload]));
		
		std::string link;
		while ("retrying upload") {
			auto & worker = takeworkerout(skynet_multiportal::upload);
			try {
				std::string link = worker.portal->upload(filename, files, timeout);
				putworkerback(worker, size);
				return link;
			} catch(std::runtime_error const & e) {
				std::cerr << worker.portal->options.url << ": " << e.what() << std::endl;
				putworkerback(worker, 0);
				if (fail) { return {}; }
			}
		}
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

	struct worker {
		size_t index;
		std::unique_ptr<skynet> portal;
		skynet_multiportal::transfer transfer;
	};
	
	std::vector<worker> workers[2];
	std::vector<size_t> free[2];

	worker & takeworkerout(skynet_multiportal::transfer_kind kind) {
		worker * w;
		{
			std::unique_lock<std::mutex> lock(worker_lists);
			while (!free[kind].size()) {
				worker_free.wait(lock);
			}
			w = &workers[kind][free[kind].back()];
			free[kind].pop_back();
		}
		w->transfer = multiportal.begin_transfer(kind);
		w->portal->options = w->transfer.portal;
		return *w;
	}

	void putworkerback(worker & w, size_t size) {
		multiportal.end_transfer(w.transfer, size);
		{
			std::unique_lock<std::mutex> lock(worker_lists);
			free[w.transfer.kind].push_back(w.index);
		}
		worker_free.notify_all();
	}
};

}
