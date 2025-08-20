#pragma once

#include "member.h"
#include "hasher.h"
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>

namespace consistent {

constexpr int DEFAULT_PARTITION_COUNT = 271;
constexpr int DEFAULT_REPLICATION_FACTOR = 20;
constexpr double DEFAULT_LOAD = 1.25;

class InsufficientMemberCountException : public std::runtime_error {
public:
    InsufficientMemberCountException(const std::string& msg) : std::runtime_error(msg) {}
};

class InsufficientSpaceException : public std::runtime_error {
public:
    InsufficientSpaceException(const std::string& msg) : std::runtime_error(msg) {}
};

// Config .
struct Config {
    std::unique_ptr<Hasher> hasher;
    int partition_count = DEFAULT_PARTITION_COUNT;
    int replication_factor = DEFAULT_REPLICATION_FACTOR;
    double load = DEFAULT_LOAD;
    
    Config() = default;
    Config(std::unique_ptr<Hasher> h, int pc = DEFAULT_PARTITION_COUNT, 
           int rf = DEFAULT_REPLICATION_FACTOR, double l = DEFAULT_LOAD)
        : hasher(std::move(h)), partition_count(pc), replication_factor(rf), load(l) {}
};

// Consistent hash ring
class Consistent {
private:
    mutable std::shared_mutex mutex_;
    
    Config config_;
    std::vector<uint64_t> sorted_set_;
    uint64_t partition_count_;
    std::unordered_map<std::string, double> loads_;
    std::unordered_map<std::string, std::unique_ptr<Member>> members_;
    std::unordered_map<int, Member*> partitions_;
    std::unordered_map<uint64_t, Member*> ring_;
    
    std::vector<Member*> cached_members_;
    bool members_dirty_ = true;

    void InitMember(const Member& member);
    void DistributePartitions();
    void DistributeWithLoad(int part_id, int idx, 
                           std::unordered_map<int, Member*>& partitions,
                           std::unordered_map<std::string, double>& loads);
    
    std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
    CalculatePartitionsWithRingAndMemberCount(
        const std::unordered_map<uint64_t, Member*>& ring,
        const std::vector<uint64_t>& sorted_set,
        int member_count);
    
    // Member management helpers
    std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
    CalculatePartitionsWithNewMember(const Member& new_member);
    
    std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
    CalculatePartitionsWithoutMember(const std::string& member_name);
    
    void AddToRing(const Member& member);
    void RemoveFromRing(const std::string& name);
    void DelSlice(uint64_t val);
    
    // Key location helpers
    int GetPartitionID(const std::vector<uint8_t>& key) const;
    int GetPartitionID(const std::string& key) const;
    Member* GetPartitionOwner(int part_id) const;
    std::vector<Member*> GetClosestN(int part_id, int count) const;
    
    double AverageLoad() const;
    std::vector<uint8_t> BuildVirtualNodeKey(const std::string& member_str, int index) const;
    
    // Validation
    static void ValidateConfig(int member_count, const Config& config);

public:
    Consistent(const std::vector<std::unique_ptr<Member>>& members, Config config);
    
    void Add(std::unique_ptr<Member> member);
    void Remove(const Member& member);
    void RemoveByName(const std::string& name);

    // Note: Returned pointers are non-owning and managed by this Consistent object.
    // Do NOT delete these pointers. Do NOT use them after this object is modified or destroyed.
    Member* LocateKey(const std::vector<uint8_t>& key) const;
    Member* LocateKey(const std::string& key) const;
    std::vector<Member*> GetClosestN(const std::vector<uint8_t>& key, int count) const;
    std::vector<Member*> GetClosestN(const std::string& key, int count) const;
    
    std::vector<std::unique_ptr<Member>> GetMembers() const;
    std::unordered_map<std::string, double> LoadDistribution() const;
    double GetAverageLoad() const;
};

} // namespace consistent
