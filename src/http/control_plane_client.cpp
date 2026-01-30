#include "http/control_plane_client.hpp"
#include "utils/posthog_logger.hpp"

#include <curl/curl.h>
#include <nlohmann/json.hpp>

#include "duckdb/common/exception.hpp"

namespace posthog {

namespace {

// Callback for curl to write response data
size_t WriteCallback(void *contents, size_t size, size_t nmemb, std::string *output) {
	size_t total_size = size * nmemb;
	output->append(static_cast<char *>(contents), total_size);
	return total_size;
}

// RAII wrapper for curl easy handle
class CurlHandle {
public:
	CurlHandle() : handle_(curl_easy_init()) {
		if (!handle_) {
			throw duckdb::IOException("PostHog: Failed to initialize curl");
		}
	}

	~CurlHandle() {
		if (handle_) {
			curl_easy_cleanup(handle_);
		}
	}

	CurlHandle(const CurlHandle &) = delete;
	CurlHandle &operator=(const CurlHandle &) = delete;

	CURL *get() const {
		return handle_;
	}

private:
	CURL *handle_;
};

// RAII wrapper for curl slist
class CurlHeaders {
public:
	CurlHeaders() : headers_(nullptr) {
	}

	~CurlHeaders() {
		if (headers_) {
			curl_slist_free_all(headers_);
		}
	}

	CurlHeaders(const CurlHeaders &) = delete;
	CurlHeaders &operator=(const CurlHeaders &) = delete;

	void append(const std::string &header) {
		headers_ = curl_slist_append(headers_, header.c_str());
	}

	curl_slist *get() const {
		return headers_;
	}

private:
	curl_slist *headers_;
};

} // namespace

ControlPlaneResponse ControlPlaneClient::CreateSession(const std::string &control_plane_url,
                                                       const std::string &bearer_token,
                                                       const std::string &database_name) {
	// Build the full URL
	std::string url = control_plane_url;
	if (!url.empty() && url.back() == '/') {
		url.pop_back();
	}
	url += SESSION_ENDPOINT;

	POSTHOG_LOG_DEBUG("Control plane session request: POST %s", url.c_str());

	// Build request body
	nlohmann::json request_body;
	request_body["database"] = database_name;
	std::string body_str = request_body.dump();

	// Initialize curl
	CurlHandle curl;
	std::string response_body;
	long http_code = 0;

	// Set up headers
	CurlHeaders headers;
	headers.append("Content-Type: application/json");
	headers.append("Accept: application/json");
	headers.append("Authorization: Bearer " + bearer_token);

	// Configure curl request
	curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl.get(), CURLOPT_POST, 1L);
	curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body_str.c_str());
	curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, static_cast<long>(body_str.size()));
	curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
	curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, WriteCallback);
	curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response_body);
	curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT, static_cast<long>(DEFAULT_TIMEOUT_SECONDS));
	curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 1L);

	// Perform the request
	CURLcode res = curl_easy_perform(curl.get());
	if (res != CURLE_OK) {
		throw duckdb::IOException("PostHog: Control plane request failed: " + std::string(curl_easy_strerror(res)));
	}

	curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &http_code);
	POSTHOG_LOG_DEBUG("Control plane response: HTTP %ld", http_code);

	// Handle HTTP errors
	if (http_code == 401 || http_code == 403) {
		throw duckdb::InvalidInputException("PostHog: Authentication failed. Check your API token.");
	}
	if (http_code == 404) {
		throw duckdb::InvalidInputException("PostHog: Database '" + database_name +
		                                    "' not found or control plane endpoint not available.");
	}
	if (http_code >= 400) {
		std::string error_msg = "PostHog: Control plane returned HTTP " + std::to_string(http_code);
		try {
			auto error_json = nlohmann::json::parse(response_body);
			if (error_json.contains("error")) {
				error_msg += ": " + error_json["error"].get<std::string>();
			}
		} catch (...) {
			// Ignore JSON parse errors for error messages
		}
		throw duckdb::IOException(error_msg);
	}

	// Parse response JSON
	nlohmann::json response;
	try {
		response = nlohmann::json::parse(response_body);
	} catch (const nlohmann::json::parse_error &e) {
		throw duckdb::IOException("PostHog: Invalid JSON response from control plane: " + std::string(e.what()));
	}

	// Extract required field
	if (!response.contains("flight_endpoint") || !response["flight_endpoint"].is_string()) {
		throw duckdb::IOException("PostHog: Control plane response missing required 'flight_endpoint' field");
	}

	ControlPlaneResponse result;
	result.flight_endpoint = response["flight_endpoint"].get<std::string>();

	// Extract optional fields
	if (response.contains("session_token") && response["session_token"].is_string()) {
		result.session_token = response["session_token"].get<std::string>();
	}
	if (response.contains("expires_at") && response["expires_at"].is_string()) {
		result.expires_at = response["expires_at"].get<std::string>();
	}

	POSTHOG_LOG_INFO("Control plane returned flight endpoint: %s", result.flight_endpoint.c_str());

	return result;
}

} // namespace posthog
