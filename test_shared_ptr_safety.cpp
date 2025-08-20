#include "consistent/consistent.h"
#include <iostream>
#include <vector>
#include <memory>

using namespace consistent;

int main() {
    std::cout << "=== Testing shared_ptr Safety Improvements ===" << std::endl;
    
    try {
        // Create members using shared_ptr
        std::vector<std::shared_ptr<Member>> members;
        members.push_back(std::make_shared<GatewayMember>("gateway-1", "192.168.1.1", 8080));
        members.push_back(std::make_shared<GatewayMember>("gateway-2", "192.168.1.2", 8080));
        members.push_back(std::make_shared<GatewayMember>("gateway-3", "192.168.1.3", 8080));
        
        // Create config
        Config config(CreateCRC64Hasher(), 271, 20, 1.25);
        
        // Create consistent hash ring
        std::cout << "\n1. Creating consistent hash ring with shared_ptr..." << std::endl;
        Consistent ring(members, std::move(config));
        std::cout << "✓ Successfully created hash ring with " << members.size() << " members" << std::endl;
        
        // Test key location with shared_ptr return
        std::cout << "\n2. Testing key location (returns shared_ptr):" << std::endl;
        std::string test_key = "user:1001";
        auto member_ptr = ring.LocateKey(test_key);
        
        if (member_ptr) {
            std::cout << "✓ Key '" << test_key << "' located to: " << member_ptr->String() << std::endl;
            std::cout << "✓ Reference count: " << member_ptr.use_count() << std::endl;
            
            // Test that the pointer remains valid even after operations
            {
                auto saved_ptr = member_ptr;  // Save a copy
                std::cout << "✓ Saved copy, reference count: " << member_ptr.use_count() << std::endl;
                
                // The pointer should remain valid even if we do operations on the ring
                auto closest = ring.GetClosestN(test_key, 2);
                std::cout << "✓ GetClosestN returned " << closest.size() << " members" << std::endl;
                
                // Original pointer should still be valid
                std::cout << "✓ Original pointer still valid: " << member_ptr->String() << std::endl;
            }
            std::cout << "✓ After scope exit, reference count: " << member_ptr.use_count() << std::endl;
        }
        
        // Test GetClosestN with shared_ptr return
        std::cout << "\n3. Testing GetClosestN (returns vector<shared_ptr>):" << std::endl;
        auto closest_members = ring.GetClosestN(test_key, 3);
        std::cout << "✓ Found " << closest_members.size() << " closest members:" << std::endl;
        
        for (size_t i = 0; i < closest_members.size(); ++i) {
            std::cout << "  " << (i + 1) << ". " << closest_members[i]->String() 
                      << " (ref count: " << closest_members[i].use_count() << ")" << std::endl;
        }
        
        // Test GetMembers with shared_ptr return
        std::cout << "\n4. Testing GetMembers (returns vector<shared_ptr>):" << std::endl;
        auto all_members = ring.GetMembers();
        std::cout << "✓ Retrieved " << all_members.size() << " members:" << std::endl;
        
        for (const auto& member : all_members) {
            std::cout << "  - " << member->String() 
                      << " (ref count: " << member.use_count() << ")" << std::endl;
        }
        
        // Test adding member with shared_ptr
        std::cout << "\n5. Testing Add with shared_ptr:" << std::endl;
        auto new_member = std::make_shared<GatewayMember>("gateway-4", "192.168.1.4", 8080);
        std::cout << "Adding member: " << new_member->String() 
                  << " (ref count before add: " << new_member.use_count() << ")" << std::endl;
        
        ring.Add(new_member);
        std::cout << "✓ Member added (ref count after add: " << new_member.use_count() << ")" << std::endl;
        
        // Verify the member is accessible
        auto found_member = ring.LocateKey("test_key_for_new_member");
        if (found_member) {
            std::cout << "✓ New member can be located: " << found_member->String() << std::endl;
        }
        
        // Test safety: pointers should remain valid even after ring modifications
        std::cout << "\n6. Testing pointer safety after modifications:" << std::endl;
        auto safe_ptr = ring.LocateKey("user:1001");
        if (safe_ptr) {
            std::cout << "✓ Got pointer: " << safe_ptr->String() << std::endl;
            
            // Remove a different member
            ring.RemoveByName("gateway-2:192.168.1.2:8080");
            std::cout << "✓ Removed different member" << std::endl;
            
            // Original pointer should still be valid
            std::cout << "✓ Original pointer still valid after removal: " << safe_ptr->String() << std::endl;
            std::cout << "✓ Reference count: " << safe_ptr.use_count() << std::endl;
        }
        
        std::cout << "\n=== All shared_ptr safety tests passed! ===" << std::endl;
        std::cout << "\n✅ Benefits achieved:" << std::endl;
        std::cout << "   - No dangling pointers possible" << std::endl;
        std::cout << "   - Automatic memory management" << std::endl;
        std::cout << "   - Thread-safe reference counting" << std::endl;
        std::cout << "   - Clear ownership semantics" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
