#include <fstream>
#include <sstream>
#include <getopt.h>

#include <nlohmann/json.hpp>

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
	if (!file.is_open()) {
		throw std::runtime_error("Couldn't open " + tmpfilename + ":" + strerror(errno));
	}
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
