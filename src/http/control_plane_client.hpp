#pragma once

#include <string>
#include <optional>

namespace posthog {

/**
 * Response from the control plane POST /v1/session endpoint.
 *
 * Expected JSON schema:
 * {
 *   "flight_endpoint": "grpc://host:port",  // Required: Flight SQL server endpoint
 *   "session_token": "...",                 // Optional: Scoped session token (if different from API token)
 *   "expires_at": "2024-01-01T00:00:00Z"    // Optional: Session expiration time (ISO 8601)
 * }
 */
struct ControlPlaneResponse {
	std::string flight_endpoint;
	std::optional<std::string> session_token;
	std::optional<std::string> expires_at;
};

/**
 * HTTP client for communicating with the PostHog control plane.
 */
class ControlPlaneClient {
public:
	/**
	 * Create a session with the control plane.
	 *
	 * @param control_plane_url Base URL of the control plane (e.g., "https://api.posthog.com")
	 * @param bearer_token      API token for authentication
	 * @param database_name     Name of the database to connect to
	 * @return ControlPlaneResponse containing flight endpoint and optional session token
	 * @throws duckdb::IOException on network errors
	 * @throws duckdb::InvalidInputException on authentication or validation errors
	 */
	static ControlPlaneResponse CreateSession(const std::string &control_plane_url, const std::string &bearer_token,
	                                          const std::string &database_name);

private:
	static constexpr int DEFAULT_TIMEOUT_SECONDS = 30;
	static constexpr const char *SESSION_ENDPOINT = "/v1/session";
};

} // namespace posthog
