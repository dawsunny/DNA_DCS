//
//  Trie.h
//  FastqC
//
//  Created by bxz on 16/5/20.
//  Copyright © 2016年 bxz. All rights reserved.
//

#ifndef Trie_h
#define Trie_h

#include "ds_defs.h"

struct CTrieNode {
    uint32 rec_no;
    uint32 rec_no1, rec_no2, rec_no3, rec_no4;
    uint32 rec_offset;
    uint32 rec_offset1, rec_offset2, rec_offset3, rec_offset4;
    uint32 match_len_total;
    uint32 match_len1;
    uint32 match_len2;
    uint32 match_len3;
    uint32 match_len4;
    CTrieNode **children;
    CTrieNode() {
        rec_no = 0;
        rec_no1 = 0;
        rec_no2 = 0;
        rec_no3 = 0;
        rec_no4 = 0;
        rec_offset = 0;
        rec_offset1 = 0;
        rec_offset2 = 0;
        rec_offset3 = 0;
        rec_offset4 = 0;
        match_len_total = 0;
        match_len1 = 0;
        match_len2 = 0;
        match_len3 = 0;
        match_len4 = 0;
        children = new CTrieNode*[4];
        if (children) {
            children[0] = NULL;
            children[1] = NULL;
            children[2] = NULL;
            children[3] = NULL;
        }
    }
};

class CTrie {
    //static uint32 MIN_LEN;
    CTrieNode *root;
public:
    CTrie();
    ~CTrie();
    
    bool insert(unsigned char *str, uint32 str_len, uint32 rec_no, uint32 rec_offset);
    bool search(unsigned char *str, uint32 str_len, uint32 &rec_no, uint32 &match_len, uint32 &rec_offset);
    void deleteAll(CTrieNode *node);
    
    
    static uint32 MIN_MATCH_LEN;
    static uint32 MIN_SEARCH_LEN;
    static uint32 INSERT_LEN;
    static uint32 INSERT_STEP;
    static uint32 INSERT_QUA_THR;   //insert quality threshold
};

#endif /* Trie_h */
