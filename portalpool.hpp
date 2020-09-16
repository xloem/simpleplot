#include <siaskynet_multiportal.hpp>

#include <thread>

namespace sia {

class portalpool {
public:
	portalpool(double bytes_bandwidth_down = 1024, double bytes_bandwidth_up = 1024, size_t num_simultaneous_xfers = 4)
	: bandwidth{bytes_bandwidth_down / num_simultaneous_xfers, bytes_bandwidth_up / num_simultaneous_xfers},
	  all_workers(num_simultaneous_xfers)
	{
		for (auto & worker: all_workers) {
			free_workers.push_back(&worker);
		}
	}

	skynet::response download(std::string const & skylink, std::initializer_list<std::pair<size_t, size_t>> ranges = {}, size_t maxsize = 1024*1024*64, bool fail = false)
	{
		auto timeout = std::chrono::milliseconds((unsigned long)(1000 * maxsize / bandwidth[skynet_multiportal::download]));

		auto & worker = takeworkerout();
		while ("retrying download") {
			auto xfer = multiportal.begin_transfer(skynet_multiportal::download);
			worker.portal.options = xfer.portal;
			try {
				auto response = worker.portal.download(skylink, ranges, timeout);
				multiportal.end_transfer(xfer, response.data.size() + response.filename.size());
				putworkerback(worker);
				return response;
			} catch(std::runtime_error const & e) {
				std::cerr << xfer.portal.url << ": " << e.what() << std::endl;
				multiportal.end_transfer(xfer, 0);
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
		auto & worker = takeworkerout();
		while ("retrying upload") {
			auto xfer = multiportal.begin_transfer(skynet_multiportal::upload);
			worker.portal.options = xfer.portal;
			try {
				std::string link = worker.portal.upload(filename, files, timeout);
				multiportal.end_transfer(xfer, size);
				putworkerback(worker);
				return link;
			} catch(std::runtime_error const & e) {
				std::cerr << xfer.portal.url << ": " << e.what() << std::endl;
				multiportal.end_transfer(xfer, 0);
				if (fail) { return {}; }
			}
		}
	}
	
private:
	double bandwidth[2];
	sia::skynet_multiportal multiportal;

	struct worker {
		skynet portal;
		std::mutex mutex;
	};
	std::vector<worker> all_workers;
	std::vector<worker*> free_workers;
	std::mutex worker_lists;
	std::condition_variable worker_free;

	worker & takeworkerout() {
		std::unique_lock<std::mutex> lock(worker_lists);
		if (!free_workers.size()) {
			worker_free.wait(lock, [this](){
				return free_workers.size() > 0;
			});
		}
		worker * w = free_workers.back();
		free_workers.pop_back();
		return *w;
	}

	void putworkerback(worker & w) {
		std::unique_lock<std::mutex> lock(worker_lists);
		free_workers.push_back(&w);
		worker_free.notify_all();
	}
};

}
