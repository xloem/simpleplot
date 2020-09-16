#include "skystream.hpp"

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
	return nlohmann::json::parse(ss.str());
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
		{"down", required_argument, 0, 'd'},
		{"span", required_argument, 0, 's'},
		{"offset", required_argument, 0, 'o'},
		{"length", required_argument, 0, 'l'},
		{"help", no_argument, 0, 'h'}
	});
	if (!options.count("down") && !options.count("up")) {
		if (options.count("pos1")) {
			options["down"] = options["pos1"];
		} else {
			options["up"] = "";
		}
	}
	if (options.count("help")) {
		std::cerr << "Specify --up or --down json file; see source for options." << std::endl;
		return 0;
	}

	if (options.count("down") && options.count("up")) {
		std::cerr << "Launch two processes to both upload and download." << std::endl;
		return -1;
	}

	std::cerr << "Measuring mirror speed ..." << std::endl;
	sia::portalpool pool;
	double offset = 0;
	if (options.count("offset")) {
		offset = std::stoull(options["offset"]);
	}
	double length = 0;
	if (options.count("length")) {
		length = std::stoull(options["length"]);
	}
	std::string span = "bytes";
	if (options.count("span")) {
		span = options["span"];
	}
	if (options.count("down")) {
		std::cerr << "Downloading from " << options["down"] << " to stdout starting from " << span << " " << offset << std::endl;
		skystream stream(file2json(options["down"]), pool);

		auto range = stream.span(span);
		if (range.first > offset) {
			offset = range.first;
		}
		if (length == 0 || length + offset > range.second) {
			length = range.second - offset;
		}
		double end = offset + length;
		while (offset < end) {
			auto data = stream.read(span, offset);
			size_t suboffset = 0;
			while (suboffset < data.size()) {
				ssize_t size = write(1, data.data() + suboffset, data.size() - suboffset);
				if (size < 0) {
					perror("write");
					json2file(stream.identifiers(), options["down"]);
					return size;
				}
				suboffset += size;
			}
		}
		json2file(stream.identifiers(), options["down"]);
	} else if (options.count("up")) {
		if (!options["up"].size()) {
			options["up"] = "skystream.json";
		}
		std::cerr << "Uploading to " << options["up"] << " from stdout starting from " << span << " " << offset << std::endl;
		std::unique_ptr<skystream> streamptr;
		try {
			streamptr.reset(new skystream(file2json(options["up"]), pool));
		} catch (nlohmann::detail::parse_error) {
			streamptr.reset(new skystream(pool));
		}
		skystream & stream = *streamptr;
		auto range = stream.span(span);
		double offset = range.second;
		std::vector<uint8_t> data;
		data.reserve(1024*1024*16);
		data.resize(data.capacity());
		ssize_t size;
		while ((size = read(0, data.data(), data.size()))) {
			if (size < 0) {
				perror("read");
				json2file(stream.identifiers(), options["up"]);
				return size;
			}
			data.resize(size);
			stream.write(data, "bytes", offset);
			offset += data.size();
			data.resize(data.capacity());
		}
		json2file(stream.identifiers(), options["up"]);
	}
}
