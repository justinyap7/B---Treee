#pragma once

#include <cstddef>
#include <cstring>
#include <functional>
#include <iostream>
#include <optional>

#include "buffer/buffer_manager.h"
#include "common/defer.h"
#include "common/macros.h"
#include "storage/segment.h"

#define UNUSED(p)  ((void)(p))

namespace buzzdb {

template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
    struct Node {

        /// The level in the tree.
        uint16_t level;

        /// The number of children.
        uint16_t count;

        // Constructor
        Node(uint16_t level, uint16_t count)
            : level(level), count(count) {}

        /// Is the node a leaf node?
        bool is_leaf() const { return level == 0; }
    };

    struct InnerNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];

        /// The children.
        uint64_t children[kCapacity];

        /// Constructor.
        InnerNode() : Node(0, 0) {}


        /// Get the index of the first key that is not less than the provided key.
        /// @param[in] key       The key to be checked against.
        std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
            if (this->count == 0) {
                return {0, false};
            }
            uint32_t start = 0;
            uint32_t end = this->count - 1;
            while (start <= end) {
                uint32_t center = start + (end - start) / 2;

                if (keys[center] == key) {
                    return {center, true};
                }
                if (keys[center] < key) {
                    start = center + 1;
                } else {
                    if (center == 0 || keys[center - 1] < key) {
                        return {center, true};
                    }
                    end = center - 1;
                }
            }

            return {this->count, false};
        }


        /// Insert a key and its associated child.
        /// @param[in] key       The key to be inserted.
        /// @param[in] split_page   The associated child to be inserted.
        void insert(const KeyT& key, uint64_t split_page) {
            auto [insertPos, keyExists] = this->lower_bound(key);
            std::vector<uint64_t> childVec = get_child_vector();
            std::vector<KeyT> keyVec = get_key_vector();

            if (this->count == 1) {
                childVec.push_back(split_page);
            } else {
                if (keyExists){
                    auto keyTemp = keys[insertPos];
                    auto posTemp = static_cast<uint32_t>(this->count);
                    if (key > keyTemp && insertPos < posTemp) {
                        childVec[insertPos] = split_page;
                    } else {
                        keyVec.insert(keyVec.begin() + insertPos, key);
                        childVec.insert(childVec.begin() + insertPos + 1, split_page);
                    }
                } else {
                    childVec.push_back(split_page);
                    keyVec.push_back(key);
                }
            }
            this->count++;
            std::copy(childVec.begin(), childVec.end(), children);
            std::copy(keyVec.begin(), keyVec.end(), keys);
        }


        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return              The separator key.
        KeyT split(std::byte* buffer) {
            auto *right_inner_node = new (buffer) InnerNode();
            uint32_t split_point = this->count / 2;
            KeyT split_key = keys[split_point - 1];
            right_inner_node->level = this->level;
            right_inner_node->count = split_point;
            auto tempNum = this->count - split_point;
            memcpy(right_inner_node->children, &children[split_point], tempNum * sizeof(uint64_t));
            memcpy(right_inner_node->keys, &keys[split_point], tempNum * sizeof(KeyT));
            this->count = split_point;
            return split_key;
        }

        /// Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            uint16_t keyCount = this->count;
            if (keyCount > 0) {
                --keyCount;
            }
            return {keys, keys + keyCount};
        }

        /// Returns the values.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<uint64_t> get_child_vector() {
            return std::vector<uint64_t>(children, children + this->count);
        }
    };

    struct LeafNode: public Node {
        /// The capacity of a node.
        /// TODO think about the capacity that the nodes have.
        static constexpr uint32_t kCapacity = 42;

        /// The keys.
        KeyT keys[kCapacity];
        /// The values.
        ValueT values[kCapacity];

        /// Constructor.
        LeafNode() : Node(0, 0) {}



        std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
            if (this->count == 0) {
                return {0, false};
            }
            uint32_t start = 0;
            uint32_t end = this->count - 1;
            while (start <= end) {
                uint32_t center = start + (end - start) / 2;

                if (keys[center] == key) {
                    return {center, true};
                }
                if (keys[center] < key) {
                    start = center + 1;
                } else {
                    if (center == 0 || keys[center - 1] < key) {
                        return {center, true};
                    }
                    end = center - 1;
                }
            }
            return {this->count, false};
        }

        /// Insert a key.
        /// @param[in] key          The key that should be inserted.
        /// @param[in] value        The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            auto [insertPos, keyExists] = this->lower_bound(key);
            std::vector<uint64_t> valueVec = get_value_vector();
            std::vector<KeyT> keyVec = get_key_vector();
            if (keyExists && key >= keys[insertPos]) {
                valueVec[insertPos] = value;
            } else {
                if (keyExists) {
                    keyVec.insert(keyVec.begin() + insertPos, key);
                    valueVec.insert(valueVec.begin() + insertPos, value);
                } else {
                    keyVec.push_back(key);
                    valueVec.push_back(value);
                }
                this->count++;
            }
            syncArrays(keyVec, valueVec);
        }

        void syncArrays(const std::vector<KeyT>& keyVec, const std::vector<ValueT>& valueVec) {
            for (size_t i = 0; i < this->count; i++) {
                keys[i] = keyVec[i];
                values[i] = valueVec[i];
            }
        }

        /// Erase a key.
        void erase(const KeyT &key) {
            uint32_t idx;
            bool isKeyPresent = locateKeyPosition(key, idx);
            
            if (isKeyPresent) {
                moveDataToLeftFrom(idx);
            }
        }

        bool locateKeyPosition(const KeyT &keyToLocate, uint32_t &position) {
            auto searchResult = this->lower_bound(keyToLocate);
            position = searchResult.second ? searchResult.first : this->count - 1;
            return searchResult.second && (keys[position] == keyToLocate);
        }

        void moveDataToLeftFrom(uint32_t startIndex) {
            std::memmove(keys + startIndex, keys + startIndex + 1, (this->count - startIndex) * sizeof(KeyT));
            std::memmove(values + startIndex, values + startIndex + 1, (this->count - startIndex) * sizeof(ValueT));
            --this->count;
        }

        /// Split the node.
        /// @param[in] buffer       The buffer for the new page.
        /// @return                 The separator key.
        KeyT split(std::byte* buffer) {
            int halfPoint = (this->count - 1) / 2;
            LeafNode* newLeaf = createAndInitializeLeaf(buffer, halfPoint);
            
            KeyT separatorKey = keys[halfPoint + 1];

            transferDataToNewLeaf(halfPoint, newLeaf);

            return separatorKey;
        }

        LeafNode* createAndInitializeLeaf(std::byte* buffer, int half) {
            LeafNode* leaf = reinterpret_cast<LeafNode*>(buffer);
            leaf->count = half;
            this->count -= half;
            return leaf;
        }

        void transferDataToNewLeaf(int startPoint, LeafNode* leaf) {
            std::copy(values + this->count, values + this->count + startPoint + 1, leaf->values);
            std::copy(keys + this->count, keys + this->count + startPoint, leaf->keys);
        }

        // Returns the keys.
        /// Can be implemented inefficiently as it's only used in the tests.
        std::vector<KeyT> get_key_vector() {
            return std::vector<KeyT>(std::begin(keys), std::begin(keys) + this->count);
        }
        

        /// Returns the values.
        std::vector<ValueT> get_value_vector() {
            return std::vector<KeyT>(std::begin(values), std::begin(values) + this->count);
        }
    };

    /// The root.
    std::optional<uint64_t> root;

    /// Next page id.
    /// You don't need to worry about about the page allocation.
    /// Just increment the next_page_id whenever you need a new page.
    uint64_t next_page_id;

    /// Constructor.
    BTree(uint16_t segment_id, BufferManager &buffer_manager)
        : Segment(segment_id, buffer_manager) {
        next_page_id = 1;
    }


    /// Lookup an entry in the tree.
    /// @param[in] key      The key that should be searched.
    /// @return             Whether the key was in the tree.
    std::optional<ValueT> lookup(const KeyT &key) {
        if (!root) return {};

        BufferFrame* currentFrame = &buffer_manager.fix_page(root.value(), false);
        Node* currentNode = reinterpret_cast<Node*>(currentFrame->get_data());

        while (!currentNode->is_leaf()) {
            InnerNode* inner = reinterpret_cast<InnerNode*>(currentNode);

            uint32_t idx;
            bool exactMatch;
            std::tie(idx, exactMatch) = inner->lower_bound(key);

            if (!exactMatch) idx = currentNode->count - 1;

            // Lock coupling
            uint64_t nextPageId = inner->children[idx];
            BufferFrame* nextFrame = &buffer_manager.fix_page(nextPageId, false);
            buffer_manager.unfix_page(*currentFrame, false);
            currentFrame = nextFrame;
            currentNode = reinterpret_cast<Node*>(currentFrame->get_data());
        }

        LeafNode* leaf = reinterpret_cast<LeafNode*>(currentNode);
        auto [valueIdx, found] = leaf->lower_bound(key);
        buffer_manager.unfix_page(*currentFrame, false);

        return (found && leaf->keys[valueIdx] == key) ? std::make_optional(leaf->values[valueIdx]) : std::nullopt;
    }

    /// Erase an entry in the tree.
    /// @param[in] key      The key that should be searched.
    void erase(const KeyT &key) {
        if (!root) return;

        std::tuple<BufferFrame*, Node*> current = get_initial_node(root.value());
        std::tuple<BufferFrame*, Node*> parent = { nullptr, nullptr };

        while (!std::get<1>(current)->is_leaf()) {
            parent = current;
            current = navigate_to_child(key, current);
        }

        LeafNode* leaf = reinterpret_cast<LeafNode*>(std::get<1>(current));
        leaf->erase(key);

        if (leaf->count <= 0 && std::get<0>(parent)) {
            remove_from_parent(key, parent);
        }
        
        buffer_manager.unfix_page(*std::get<0>(current), true);
    }

    std::tuple<BufferFrame*, Node*> get_initial_node(uint64_t pageID) {
        BufferFrame* currentFrame = &buffer_manager.fix_page(pageID, false);
        Node* currentNode = reinterpret_cast<Node*>(currentFrame->get_data());
        return { currentFrame, currentNode };
    }

    std::tuple<BufferFrame*, Node*> navigate_to_child(const KeyT& key, const std::tuple<BufferFrame*, Node*>& current) {
        InnerNode* inner = reinterpret_cast<InnerNode*>(std::get<1>(current));
        auto [childIdx, exactMatch] = inner->lower_bound(key);
        if (!exactMatch) childIdx = std::get<1>(current)->count - 1;
        
        uint64_t nextPage = inner->children[childIdx];
        buffer_manager.unfix_page(*std::get<0>(current), false);

        return get_initial_node(nextPage);
    }

    void remove_from_parent(const KeyT& key, const std::tuple<BufferFrame*, Node*>& parent) {
        InnerNode* parentNode = reinterpret_cast<InnerNode*>(std::get<1>(parent));
        auto [parentIdx, found] = parentNode->lower_bound(key);
        if (found && parentNode->keys[parentIdx] == key) {
            auto temp = parentNode->count - parentIdx;
            memmove(parentNode->keys + parentIdx, parentNode->keys + parentIdx + 1, temp * sizeof(KeyT));
            memmove(parentNode->children + parentIdx, parentNode->children + parentIdx + 1, temp * sizeof(ValueT));
            parentNode->count--;
        }
    }

    /// Inserts a new entry into the tree.
    /// @param[in] key      The key that should be inserted.
    /// @param[in] value    The value that should be inserted.
    void insert(const KeyT& key, const ValueT& value) {
        if (!root) {
            root = 0;
            next_page_id = 1;
        }

        BufferFrame* currentBuffer = &buffer_manager.fix_page(root.value(), true);
        BufferFrame* parentBuffer = nullptr;
        bool currentIsDirty = false;
        bool parentIsDirty = false;

        while (true) {
            Node* currentNode = reinterpret_cast<Node*>(currentBuffer->get_data());

            // Handle leaf node
            if (currentNode->is_leaf()) {
                LeafNode* leaf = reinterpret_cast<LeafNode*>(currentNode);

                // If there's space in the leaf, insert and exit
                if (leaf->count < 42) {
                    leaf->insert(key, value);
                    currentIsDirty = true;

                    buffer_manager.unfix_page(*currentBuffer, currentIsDirty);
                    if (parentBuffer) buffer_manager.unfix_page(*parentBuffer, parentIsDirty);
                    return;
                }

                // If leaf is full, handle the split
                uint64_t newLeafID = next_page_id++;
                BufferFrame* newLeafBuffer = &buffer_manager.fix_page(newLeafID, true);
                KeyT splitKey = leaf->split(reinterpret_cast<std::byte *>(newLeafBuffer->get_data()));
                currentIsDirty = true;

                // Update the parent node after the split
                if (!parentBuffer) {
                    uint64_t oldLeafID = root.value();
                    root = next_page_id++;
                    parentBuffer = &buffer_manager.fix_page(root.value(), true);
                    parentIsDirty = true;

                    InnerNode* rootAsInner = reinterpret_cast<InnerNode*>(parentBuffer->get_data());
                    rootAsInner->level = 1;
                    rootAsInner->insert(splitKey, oldLeafID);
                    rootAsInner->insert(splitKey, newLeafID);
                } else {
                    InnerNode* parentNode = reinterpret_cast<InnerNode*>(parentBuffer->get_data());
                    parentNode->insert(splitKey, newLeafID);
                    parentIsDirty = true;
                }

                // Decide which buffer to continue with
                buffer_manager.unfix_page((key < splitKey) ? *newLeafBuffer : *currentBuffer, currentIsDirty);
                if (key >= splitKey) currentBuffer = newLeafBuffer;

            } else { // Handle inner node
                InnerNode* inner = reinterpret_cast<InnerNode*>(currentNode);

                // If the inner node is full, split it
                if (inner->count == 42) {
                    uint64_t newInnerID = next_page_id++;
                    BufferFrame* newInnerBuffer = &buffer_manager.fix_page(newInnerID, true);
                    KeyT splitKey = inner->split(reinterpret_cast<std::byte *>(newInnerBuffer->get_data()));
                    currentIsDirty = true;

                    if (!parentBuffer) {
                        uint64_t oldInnerID = root.value();
                        root = next_page_id++;
                        parentBuffer = &buffer_manager.fix_page(root.value(), true);

                        InnerNode* rootAsInner = reinterpret_cast<InnerNode*>(parentBuffer->get_data());
                        rootAsInner->level = inner->level + 1;
                        rootAsInner->insert(splitKey, oldInnerID);
                        rootAsInner->insert(splitKey, newInnerID);
                    } else {
                        InnerNode* parentNode = reinterpret_cast<InnerNode*>(parentBuffer->get_data());
                        parentNode->insert(splitKey, newInnerID);
                        parentIsDirty = true;
                    }

                    buffer_manager.unfix_page((key < splitKey) ? *newInnerBuffer : *currentBuffer, currentIsDirty);
                    if (key >= splitKey) currentBuffer = newInnerBuffer;

                } else { // Move deeper into the tree
                    auto boundary = inner->lower_bound(key);
                    uint64_t childID = boundary.second ? inner->children[boundary.first] : inner->children[inner->count - 1];

                    if (parentBuffer) buffer_manager.unfix_page(*parentBuffer, parentIsDirty);
                    parentBuffer = currentBuffer;
                    currentBuffer = &buffer_manager.fix_page(childID, true);
                }
            }
        }
    }
};

} 
