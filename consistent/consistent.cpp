#include "consistent.h"
#include <algorithm>
#include <sstream>
#include <cmath>
#include <random>

namespace consistent {

Consistent::Consistent(const std::vector<std::unique_ptr<Member>>& members, Config config)
    : config_(std::move(config)), partition_count_(config_.partition_count) {
    
    // Set defaults
    if (config_.partition_count == 0) {
        config_.partition_count = DEFAULT_PARTITION_COUNT;
        partition_count_ = DEFAULT_PARTITION_COUNT;
    }
    if (config_.replication_factor == 0) {
        config_.replication_factor = DEFAULT_REPLICATION_FACTOR;
    }
    if (config_.load == 0.0) {
        config_.load = DEFAULT_LOAD;
    }
    
    // Validate configuration
    ValidateConfig(members.size(), config_);
    
    // Initialize members
    for (const auto& member : members) {
        InitMember(member->Clone());
    }
    
    if (!members.empty()) {
        DistributePartitions();
    }
}

void Consistent::ValidateConfig(int member_count, const Config& config) {
    if (!config.hasher) {
        throw std::invalid_argument("hasher cannot be null");
    }
    if (member_count == 0) {
        return; // Empty ring is valid
    }
    
    double avg_load = static_cast<double>(config.partition_count) / member_count * config.load;
    double max_load = std::ceil(avg_load);
    
    if (max_load > config.replication_factor * 2.0) {
        std::ostringstream oss;
        oss << "configuration may cause distribution issues: partitionCount=" 
            << config.partition_count << ", memberCount=" << member_count 
            << ", load=" << config.load << " results in avgLoad=" << max_load << " per member";
        throw std::invalid_argument(oss.str());
    }
}

void Consistent::Add(std::unique_ptr<Member> member) {
    std::string member_name = member->String();

    // First check if the member already exists (read lock)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (members_.find(member_name) != members_.end()) {
            return; // Member already exists
        }
    }

    // Acquire write lock to update data structures
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Double-check if member exists
    if (members_.find(member_name) != members_.end()) {
        return;
    }

    // CRITICAL: Take ownership first, then get stable pointer
    members_[member_name] = std::move(member);
    Member* stable_ptr = members_[member_name].get();

    // Add to ring
    AddToRing(stable_ptr);

    // Calculate new partition distribution (now that member is in members_)
    auto [new_partitions, new_loads] = CalculatePartitionsWithNewMember(member_name);

    partitions_ = std::move(new_partitions);
    loads_ = std::move(new_loads);
    members_dirty_ = true;
}

std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
Consistent::CalculatePartitionsWithNewMember(const std::string& member_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    auto it = members_.find(member_name);
    if (it == members_.end()) {
        // Member not found, return current state
        return {partitions_, loads_};
    }

    Member* new_member = it->second.get();

    // Create temporary ring and sorted set
    std::unordered_map<uint64_t, Member*> temp_ring = ring_;
    std::vector<uint64_t> temp_sorted_set = sorted_set_;

    // Add new member to temporary ring
    for (int i = 0; i < config_.replication_factor; ++i) {
        auto key = BuildVirtualNodeKey(member_name, i);
        uint64_t h = config_.hasher->Sum64(key);
        temp_ring[h] = new_member;
        temp_sorted_set.push_back(h);
    }

    // Sort the temporary set
    std::sort(temp_sorted_set.begin(), temp_sorted_set.end());

    int new_member_count = members_.size();
    return CalculatePartitionsWithRingAndMemberCount(temp_ring, temp_sorted_set, new_member_count);
}

void Consistent::AddToRing(Member* member) {
    for (int i = 0; i < config_.replication_factor; ++i) {
        auto key = BuildVirtualNodeKey(member->String(), i);
        uint64_t h = config_.hasher->Sum64(key);
        ring_[h] = member;  // Now using stable pointer from members_
        sorted_set_.push_back(h);
    }
    std::sort(sorted_set_.begin(), sorted_set_.end());
}

void Consistent::Remove(const Member& member) {
    RemoveByName(member.String());
}

void Consistent::RemoveByName(const std::string& name) {
    // First check if member exists (read lock)
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (members_.find(name) == members_.end()) {
            return; // Member doesn't exist
        }
    }

    // Calculate new partition distribution
    auto [new_partitions, new_loads] = CalculatePartitionsWithoutMember(name);

    // Acquire write lock to update data structures
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Double-check if member exists
    if (members_.find(name) == members_.end()) {
        return;
    }

    // CRITICAL: Remove all references to the member BEFORE deleting it
    // This prevents dangling pointers in ring_ and partitions_
    RemoveFromRing(name);

    // Update partitions and loads BEFORE erasing the member
    if (members_.size() == 1) {
        // Last member being removed
        partitions_.clear();
        loads_.clear();
    } else {
        partitions_ = std::move(new_partitions);
        loads_ = std::move(new_loads);
    }

    // Finally, erase the member (this destroys the unique_ptr and frees memory)
    members_.erase(name);
    members_dirty_ = true;
}

std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
Consistent::CalculatePartitionsWithoutMember(const std::string& member_name) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    // Pre-calculate hashes to delete
    std::unordered_set<uint64_t> hashes_to_delete;
    for (int i = 0; i < config_.replication_factor; ++i) {
        auto key = BuildVirtualNodeKey(member_name, i);
        uint64_t h = config_.hasher->Sum64(key);
        hashes_to_delete.insert(h);
    }
    
    // Create temporary ring and sorted set
    std::unordered_map<uint64_t, Member*> temp_ring;
    std::vector<uint64_t> temp_sorted_set;
    
    for (const auto& [hash, member] : ring_) {
        if (hashes_to_delete.find(hash) == hashes_to_delete.end()) {
            temp_ring[hash] = member;
            temp_sorted_set.push_back(hash);
        }
    }
    
    std::sort(temp_sorted_set.begin(), temp_sorted_set.end());
    
    return CalculatePartitionsWithRingAndMemberCount(temp_ring, temp_sorted_set, members_.size() - 1);
}

void Consistent::RemoveFromRing(const std::string& name) {
    for (int i = 0; i < config_.replication_factor; ++i) {
        auto key = BuildVirtualNodeKey(name, i);
        uint64_t h = config_.hasher->Sum64(key);
        ring_.erase(h);
        DelSlice(h);
    }
}

void Consistent::DelSlice(uint64_t val) {
    auto it = std::lower_bound(sorted_set_.begin(), sorted_set_.end(), val);

    if (it != sorted_set_.end() && *it == val) {
        sorted_set_.erase(it);
    }
}

Member* Consistent::LocateKey(const std::vector<uint8_t>& key) const {
    if (ring_.empty()) {
        return nullptr;
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);
    int part_id = GetPartitionID(key);
    return GetPartitionOwner(part_id);
}

Member* Consistent::LocateKey(const std::string& key) const {
    if (ring_.empty()) {
        return nullptr;
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);
    int part_id = GetPartitionID(key);
    return GetPartitionOwner(part_id);
}

int Consistent::GetPartitionID(const std::vector<uint8_t>& key) const {
    uint64_t hkey = config_.hasher->Sum64(key);
    return static_cast<int>(hkey % partition_count_);
}

int Consistent::GetPartitionID(const std::string& key) const {
    uint64_t hkey = config_.hasher->Sum64(key);
    return static_cast<int>(hkey % partition_count_);
}

std::vector<Member*> Consistent::GetClosestN(const std::vector<uint8_t>& key, int count) const {
    if (count <= 0) {
        return {};
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (count > static_cast<int>(members_.size())) {
        throw InsufficientMemberCountException("insufficient number of members");
    }

    int part_id = GetPartitionID(key);
    return GetClosestN(part_id, count);
}

std::vector<Member*> Consistent::GetClosestN(const std::string& key, int count) const {
    if (count <= 0) {
        return {};
    }

    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (count > static_cast<int>(members_.size())) {
        throw InsufficientMemberCountException("insufficient number of members");
    }

    int part_id = GetPartitionID(key);
    return GetClosestN(part_id, count);
}

std::vector<Member*> Consistent::GetClosestN(int part_id, int count) const {
    if (count > static_cast<int>(members_.size())) {
        throw InsufficientMemberCountException("insufficient number of members");
    }

    if (sorted_set_.empty()) {
        throw InsufficientMemberCountException("insufficient number of members");
    }

    Member* owner = GetPartitionOwner(part_id);
    if (!owner) {
        throw InsufficientMemberCountException("insufficient number of members");
    }

    // Hash the owner's name to find a starting position on the ring that corresponds to the owner itself.
    // This ensures the traversal for replicas starts from the primary member.
    uint64_t owner_key = config_.hasher->Sum64(owner->String());

    auto it = std::lower_bound(sorted_set_.begin(), sorted_set_.end(), owner_key);
    int start_idx = std::distance(sorted_set_.begin(), it);

    if (start_idx >= static_cast<int>(sorted_set_.size())) {
        start_idx = 0;
    }

    std::vector<Member*> result;
    result.reserve(count);
    std::unordered_set<std::string> seen;
    int idx = start_idx;

    while (static_cast<int>(result.size()) < count && static_cast<int>(seen.size()) < static_cast<int>(members_.size())) {
        uint64_t hash = sorted_set_[idx];
        Member* member = ring_.at(hash);
        std::string member_key = member->String();

        if (seen.find(member_key) == seen.end()) {
            result.push_back(member);
            seen.insert(member_key);
        }

        idx++;
        if (idx >= static_cast<int>(sorted_set_.size())) {
            idx = 0;
        }
    }

    return result;
}

Member* Consistent::GetPartitionOwner(int part_id) const {
    auto it = partitions_.find(part_id);
    return (it != partitions_.end()) ? it->second : nullptr;
}

std::vector<std::unique_ptr<Member>> Consistent::GetMembers() const {
    // First, try to check the cache with a read lock
    {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!members_dirty_ && !cached_members_.empty()) {
            // Clone from cached raw pointers
            std::vector<std::unique_ptr<Member>> result;
            result.reserve(cached_members_.size());
            for (Member* member : cached_members_) {
                result.push_back(member->Clone());
            }
            return result;
        }
    }

    // Acquire write lock to update cache
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Check again if cache was updated
    if (!members_dirty_ && !cached_members_.empty()) {
        // Clone from cached raw pointers
        std::vector<std::unique_ptr<Member>> result;
        result.reserve(cached_members_.size());
        for (Member* member : cached_members_) {
            result.push_back(member->Clone());
        }
        return result;
    }

    // Update cache with raw pointers (no cloning for cache)
    cached_members_.clear();
    cached_members_.reserve(members_.size());
    for (const auto& [name, member] : members_) {
        cached_members_.push_back(member.get());
    }
    members_dirty_ = false;

    // Clone for return value
    std::vector<std::unique_ptr<Member>> result;
    result.reserve(cached_members_.size());
    for (Member* member : cached_members_) {
        result.push_back(member->Clone());
    }

    return result;
}



std::unordered_map<std::string, double> Consistent::LoadDistribution() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return loads_;
}

double Consistent::GetAverageLoad() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return AverageLoad();
}

double Consistent::AverageLoad() const {
    if (members_.empty()) {
        return 0.0;
    }
    return static_cast<double>(partition_count_) / members_.size() * config_.load;
}

void Consistent::DistributePartitions() {
    std::unordered_map<std::string, double> loads;
    std::unordered_map<int, Member*> partitions;

    for (uint64_t part_id = 0; part_id < partition_count_; ++part_id) {
        // Convert partition ID to bytes (little endian)
        std::vector<uint8_t> bs(8);
        for (int i = 0; i < 8; ++i) {
            bs[i] = static_cast<uint8_t>((part_id >> (i * 8)) & 0xFF);
        }

        uint64_t key = config_.hasher->Sum64(bs);
        auto it = std::lower_bound(sorted_set_.begin(), sorted_set_.end(), key);
        int idx = std::distance(sorted_set_.begin(), it);

        if (idx >= static_cast<int>(sorted_set_.size())) {
            idx = 0;
        }

        DistributeWithLoad(static_cast<int>(part_id), idx, partitions, loads);
    }

    partitions_ = std::move(partitions);
    loads_ = std::move(loads);
}

void Consistent::DistributeWithLoad(int part_id, int idx,
                                   std::unordered_map<int, Member*>& partitions,
                                   std::unordered_map<std::string, double>& loads) {
    double avg_load = AverageLoad();
    int count = 0;

    while (true) {
        count++;
        if (count >= static_cast<int>(sorted_set_.size())) {
            std::ostringstream oss;
            oss << "partition " << part_id << " cannot be assigned after " << count
                << " attempts (avgLoad=" << avg_load << ", members=" << members_.size()
                << ", virtualNodes=" << sorted_set_.size() << ")";
            throw InsufficientSpaceException(oss.str());
        }

        uint64_t hash = sorted_set_[idx];
        Member* member = ring_[hash];
        double load = loads[member->String()];

        if (load + 1 <= avg_load) {
            partitions[part_id] = member;
            loads[member->String()]++;
            return;
        }

        idx++;
        if (idx >= static_cast<int>(sorted_set_.size())) {
            idx = 0;
        }
    }
}

std::pair<std::unordered_map<int, Member*>, std::unordered_map<std::string, double>>
Consistent::CalculatePartitionsWithRingAndMemberCount(
    const std::unordered_map<uint64_t, Member*>& ring,
    const std::vector<uint64_t>& sorted_set,
    int member_count) {

    std::unordered_map<std::string, double> loads;
    std::unordered_map<int, Member*> partitions;

    if (member_count == 0) {
        return {partitions, loads};
    }

    double avg_load = static_cast<double>(partition_count_) / member_count * config_.load;
    avg_load = std::ceil(avg_load);

    for (uint64_t part_id = 0; part_id < partition_count_; ++part_id) {
        // Convert partition ID to bytes (little endian)
        std::vector<uint8_t> bs(8);
        for (int i = 0; i < 8; ++i) {
            bs[i] = static_cast<uint8_t>((part_id >> (i * 8)) & 0xFF);
        }

        uint64_t key = config_.hasher->Sum64(bs);
        auto it = std::lower_bound(sorted_set.begin(), sorted_set.end(), key);
        int idx = std::distance(sorted_set.begin(), it);

        if (idx >= static_cast<int>(sorted_set.size())) {
            idx = 0;
        }

        int count = 0;
        while (true) {
            count++;
            if (count >= static_cast<int>(sorted_set.size())) {
                std::ostringstream oss;
                oss << "failed to assign partition " << part_id << " (avgLoad=" << avg_load
                    << ", members=" << member_count << ", virtualNodes=" << sorted_set.size() << ")";
                throw InsufficientSpaceException(oss.str());
            }

            uint64_t hash = sorted_set[idx];
            Member* member = ring.at(hash);
            double load = loads[member->String()];

            if (load + 1 <= avg_load) {
                partitions[static_cast<int>(part_id)] = member;
                loads[member->String()]++;
                break;
            }

            idx++;
            if (idx >= static_cast<int>(sorted_set.size())) {
                idx = 0;
            }
        }
    }

    return {partitions, loads};
}

void Consistent::InitMember(std::unique_ptr<Member> member) {
    std::string member_name = member->String();

    // Take ownership first, then get stable pointer
    members_[member_name] = std::move(member);
    Member* stable_ptr = members_[member_name].get();

    for (int i = 0; i < config_.replication_factor; ++i) {
        auto key = BuildVirtualNodeKey(member_name, i);
        uint64_t h = config_.hasher->Sum64(key);
        ring_[h] = stable_ptr;  // Use stable pointer from members_
        sorted_set_.push_back(h);
    }

    // Sort the hash values in ascending order
    std::sort(sorted_set_.begin(), sorted_set_.end());
    members_dirty_ = true;
}

std::vector<uint8_t> Consistent::BuildVirtualNodeKey(const std::string& member_str, int index) const {
    std::string index_str = std::to_string(index);
    std::vector<uint8_t> key;
    key.reserve(member_str.size() + index_str.size());

    // Append member string
    for (char c : member_str) {
        key.push_back(static_cast<uint8_t>(c));
    }

    // Append index string
    for (char c : index_str) {
        key.push_back(static_cast<uint8_t>(c));
    }

    return key;
}

} // namespace consistent
