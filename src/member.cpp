#include "member.h"
#include <sstream>

namespace consistent {

GatewayMember::GatewayMember(const std::string& id, const std::string& host, int port)
    : id_(id), host_(host), port_(port) {
}

std::string GatewayMember::String() const {
    std::ostringstream oss;
    oss << id_ << ":" << host_ << ":" << port_;
    return oss.str();
}

std::unique_ptr<Member> GatewayMember::Clone() const {
    return std::make_unique<GatewayMember>(id_, host_, port_);
}

const std::string& GatewayMember::GetID() const {
    return id_;
}

const std::string& GatewayMember::GetHost() const {
    return host_;
}

int GatewayMember::GetPort() const {
    return port_;
}

std::string GatewayMember::GetAddress() const {
    std::ostringstream oss;
    oss << host_ << ":" << port_;
    return oss.str();
}

} // namespace consistent
