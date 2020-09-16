#include "bufferedskystream.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstring>
#include <getopt.h>
#include <unistd.h>

nlohmann::json file2json(std::string filename)
{
	std::ifstream file(filename);
	std::ostringstream ss;
	ss << file.rdbuf();
	try {
		return nlohmann::json::parse(ss.str());
	} catch (nlohmann::detail::parse_error) {
		return nlohmann::json{};
	}
}

void json2file(nlohmann::json json, std::string filename)
{
	auto tmpfilename = filename + ".tmp";
	std::istringstream ss(json.dump());
	std::ofstream file(tmpfilename);
	file << ss.rdbuf();
	file.close();

	int result = std::rename(tmpfilename.c_str(), filename.c_str());
	if (result != 0) {
		throw std::runtime_error(strerror(result));
	}
}

std::map<std::string, std::string> parseoptions(int argc, char **argv, std::vector<struct option> options)
{
	options.emplace_back((struct option){0,0,0,0});
	std::string shortopts;
	for (auto & option : options) {
		shortopts += option.val;
		if (option.has_arg == required_argument) {
			shortopts += ':';
		} else if (option.has_arg == optional_argument) {
			shortopts = "::";
		}
	}
	int option_index;
	int c;
	std::map<std::string, std::string> result;
	while (-1 != (c = getopt_long(argc, argv, shortopts.c_str(), options.data(), &option_index))) {
		if (c == '?') {
			exit(-1);
		}
		auto & option = options[option_index];
		if (optarg) {
			result[option.name] = optarg;
		} else {
			result[option.name] = {};
		}
	}
	int posind = 0;
	while (optind < argc) {
		result["pos" + std::to_string(++posind)] = argv[optind];
		++ optind;
	}
	return result;
}

int main(int argc, char **argv)
{
	auto options = parseoptions(argc, argv, {
		{"up", optional_argument, 0, 'u'},
		{"down", optional_argument, 0, 'd'},
		{"size", optional_argument, 0, 's'},
		{"offset", required_argument, 0, 'o'},
		{"length", required_argument, 0, 'l'},
		{"help", no_argument, 0, 'h'}
	});
	if (!options.count("down") && !options.count("up") && !options.count("size")) {
		if (options.count("pos1")) {
			options["down"] = options["pos1"];
			std::cerr << "Assuming you mean to download from " << options["down"] << std::endl;
		} else {
			std::cerr << "Assuming you mean to upload" << std::endl;
			options["up"] = "";
		}
	}
	if (options.count("help") ) {
		std::cerr << "Specify --up or --down json file; see source for options." << std::endl;
		return 0;
	}

	if (options.count("down") + options.count("up") + options.count("size") > 1) {
		std::cerr << "Launch multiple processes to do multiple things." << std::endl;
		return -1;
	}

	std::cerr << "Finding responsive mirrors ..." << std::endl;
	sia::portalpool pool;
	uint64_t offset = 0;
	if (options.count("offset")) {
		offset = std::stoull(options["offset"]);
	}
	uint64_t length = 0;
	if (options.count("length")) {
		length = std::stoull(options["length"]);
	}
	if (options.count("size")) {
		if (!options["size"].size()) {
			options["size"] = options["pos1"];
		}
		if (!options["size"].size()) {
			options["size"] = "skystream.json";
			std::cerr << "No --size=, assuming " << options["size"] << std::endl;
		}
		skystream stream(file2json(options["size"]), pool);
		auto range = stream.span("bytes");
		std::cout << range.first << std::endl;
		std::cout << range.second << std::endl;
	}
	bufferedskystreams streams(pool);
	if (options.count("down")) {
		if (!options["down"].size()) {
			options["down"] = options["pos1"];
		}
		if (!options["down"].size()) {
			options["down"] = "skystream.json";
			std::cerr << "No --down=, assuming " << options["down"] << std::endl;
		}
		streams.add(file2json(options["down"]));
		auto & stream = streams.get(0);

		auto range = stream.span("bytes");
		if (range.first > offset) {
			offset = range.first;
		}
		if (length == 0 || length + offset > range.second) {
			length = range.second - offset;
		}
		uint64_t end = offset + length;
		std::cerr << "Downloading range [" << offset << "," << end << ") from " << options["down"] << " to stdout ..." << std::endl;
		auto pump = std::thread([&](){
			ssize_t size;
			while ((size = stream.queue_net_down()) != -1) {
				if (size) {
					std::cerr << "Finished queuing " << size << " bytes" << std::endl;
				} else {
					std::unique_lock lock(streams.read_mutex);
					streams.reading.wait(lock);
				}
			}
		});
		while (offset < end) {
			auto data = stream.xfer_local_down(offset, end);
			std::cerr << "Downloaded " << data.size() << " bytes" << std::endl;
			size_t suboffset = 0;
			while (suboffset < data.size()) {
				ssize_t size = write(1, data.data() + suboffset, data.size() - suboffset);
				if (size < 0) {
					perror("write");
					return size;
				}
				suboffset += size;
			}
			offset += data.size();
		}
		stream.shutdown();
		pump.join();
	} else if (options.count("up")) {
		if (!options["up"].size()) {
			options["up"] = options["pos1"];
		}
		if (!options["up"].size()) {
			options["up"] = "skystream.json";
			std::cerr << "No --up=, assuming " << options["up"] << std::endl;
		}
		streams.add(file2json(options["up"]));
		bufferedskystream & stream = streams.get(0);
		auto range = stream.span("bytes");
		double offset = range.second;
		std::cerr << "Uploading to " << options["up"] << " from stdin starting from " << "bytes" << " " << offset << std::endl;
		std::vector<uint8_t> data;
		data.reserve(1024*1024*16);
		data.resize(data.capacity());
		ssize_t size;
		auto pump = std::thread([&](){
			ssize_t size;
			while ((size = stream.xfer_net_up()) != -1) {
				if (size) {
					json2file(stream.identifiers(), options["up"]);
					std::cerr << "Uploaded " << size << " bytes" << std::endl;
				} else {
					std::unique_lock lock(streams.write_mutex);
					streams.writing.wait(lock);
				}
			}
		});
		while ((size = read(0, data.data(), data.size()))) {
			if (size < 0) {
				perror("read");
				return size;
			}
			data.resize(size);
			stream.queue_local_up(std::move(data));
			//std::cerr << "Queued " << size << " bytes" << std::endl;
			offset += size;
			data.resize(data.capacity());
		}
		stream.shutdown();
		pump.join();
	}
}
