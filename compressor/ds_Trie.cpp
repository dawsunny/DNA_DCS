//
//  Trie.cpp
//  FastqC
//
//  Created by bxz on 16/5/20.
//  Copyright © 2016年 bxz. All rights reserved.
//

#include <stdio.h>
#include "ds_Trie.h"

uint32 CTrie::MIN_MATCH_LEN = 10;
uint32 CTrie::MIN_SEARCH_LEN = 20;
uint32 CTrie::INSERT_LEN = 64;
uint32 CTrie::INSERT_STEP = 8;
uint32 CTrie::INSERT_QUA_THR = 70;


// ********************************************************************************************
CTrie::CTrie() {
    root = new CTrieNode();
}

// ********************************************************************************************
CTrie::~CTrie() {
    deleteAll(root);
}

// ********************************************************************************************
void CTrie::deleteAll(CTrieNode *node) {
    for (uint32 i = 0; i < 4; ++i) {
        if (node->children[i]) {
            deleteAll(node->children[i]);
        }
    }
    delete node;
}

// ********************************************************************************************
bool CTrie::insert(unsigned char* str, uint32 str_len, uint32 rec_no, uint32 rec_offset) {
    if (str_len < MIN_SEARCH_LEN) { //if < MIN_MATCH_LEN
        //return false;
    }
    CTrieNode *cur = root;
    uint32 i;
    for (i = 0; i < str_len; ++i) {
        switch (str[i]) {
            case 'A':
                if (cur->children[0]) {
                    cur = cur->children[0];
                } else {
                    CTrieNode *tmp = new CTrieNode();
                    /*
                    if (flag == 0) {
                        tmp->rec_no1 = rec_no;
                    } else {
                        tmp->rec_no2 = rec_no;
                    }
                     */
                    cur->children[0] = tmp;
                    cur = tmp;
                }
                cur->rec_offset = rec_offset;
                break;
                
            case 'C':
                if (cur->children[1]) {
                    cur = cur->children[1];
                } else {
                    CTrieNode *tmp = new CTrieNode();
                    /*
                    if (flag == 0) {
                        tmp->rec_no1 = rec_no;
                    } else {
                        tmp->rec_no2 = rec_no;
                    }
                     */
                    cur->children[1] = tmp;
                    cur = tmp;
                }
                cur->rec_offset = rec_offset;
                break;
                
            case 'G':
                if (cur->children[2]) {
                    cur = cur->children[2];
                } else {
                    CTrieNode *tmp = new CTrieNode();
                    /*
                    if (flag == 0) {
                        tmp->rec_no1 = rec_no;
                    } else {
                        tmp->rec_no2 = rec_no;
                    }
                     */
                    cur->children[2] = tmp;
                    cur = tmp;
                }
                cur->rec_offset = rec_offset;
                break;
                
            case 'T':
                if (cur->children[3]) {
                    cur = cur->children[3];
                } else {
                    CTrieNode *tmp = new CTrieNode();
                    /*
                    if (flag == 0) {
                        tmp->rec_no1 = rec_no;
                    } else {
                        tmp->rec_no2 = rec_no;
                    }
                     */
                    cur->children[3] = tmp;
                    cur = tmp;
                }
                cur->rec_offset = rec_offset;
                break;
                
            default:
                return false;
        }
    }
    return true;
}

// ********************************************************************************************
bool CTrie::search(unsigned char *str, uint32 str_len, uint32 &rec_no, uint32 &match_len, uint32 &rec_offset) {
    if (str_len < MIN_SEARCH_LEN) {     //if < MIN_MATCH_LEN
        //return false;
    }
    uint32 i;
    CTrieNode *cur = root;
    for (i = 0; i < str_len; ++i) {
        switch (str[i]) {
            case 'A':
                if (cur->children[0]) {
                    cur = cur->children[0];
                    match_len++;
                } else {
                    /*
                    if (cur->rec_no1) {
                        rec_no = cur->rec_no1;
                    } else {
                        rec_no = cur->rec_no2;
                    }
                     */
                    rec_no = cur->rec_no;
                    rec_offset = cur->rec_offset;
                    return match_len >= MIN_SEARCH_LEN;
                }
                break;
                
            case 'C':
                if (cur->children[1]) {
                    cur = cur->children[1];
                    match_len++;
                } else {
                    /*
                    if (cur->rec_no1) {
                        rec_no = cur->rec_no1;
                    } else {
                        rec_no = cur->rec_no2;
                    }
                    */
                    rec_no = cur->rec_no;
                    rec_offset = cur->rec_offset;
                    return match_len >= MIN_SEARCH_LEN;
                }
                break;
                
            case 'G':
                if (cur->children[2]) {
                    cur = cur->children[2];
                    match_len++;
                } else {
                    /*
                    if (cur->rec_no1) {
                        rec_no = cur->rec_no1;
                    } else {
                        rec_no = cur->rec_no2;
                     }
                     */
                    rec_no = cur->rec_no;
                    rec_offset = cur->rec_offset;
                    return match_len >= MIN_SEARCH_LEN;
                }
                break;
                
            case 'T':
                if (cur->children[3]) {
                    cur = cur->children[3];
                    match_len++;
                } else {
                    /*
                    if (cur->rec_no1) {
                        rec_no = cur->rec_no1;
                    } else {
                        rec_no = cur->rec_no2;
                     }
                     */
                    rec_no = cur->rec_no;
                    rec_offset = cur->rec_offset;
                    return match_len >= MIN_SEARCH_LEN;
                }
                break;
                
            default:
                return match_len >= MIN_SEARCH_LEN;
        }
    }
    return match_len >= MIN_SEARCH_LEN;
}