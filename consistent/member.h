#pragma once

#include <string>
#include <memory>

namespace consistent {

class Member {
public:
    virtual ~Member() = default;
    virtual std::string String() const = 0;
    virtual std::unique_ptr<Member> Clone() const = 0;
};

// GatewayMember .
class GatewayMember : public Member {
private:
    std::string id_;
    std::string host_;
    int port_;

public:
    GatewayMember(const std::string& id, const std::string& host, int port);
    
    std::string String() const override;
    std::unique_ptr<Member> Clone() const override;
    
    const std::string& GetID() const;
    const std::string& GetHost() const;
    int GetPort() const;
    std::string GetAddress() const;
};

} // namespace consistent
