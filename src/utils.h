/*
 * utilities.
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef UTILS_H
#define UTILS_H


#define SAFE_REQ_ATTR_FREE(Req, AttrName)   \
  if (Req->AttrName != NULL) {              \
    free(Req->AttrName);                    \
    Req->AttrName = NULL;                   \
  }                                         \

#define SAFE_REQ_ATTR_DELETE(Req, AttrName)   \
  if (Req->AttrName) {                        \
    delete Req->AttrName;                     \
    Req->AttrName = NULL;                     \
  }                                           \


#endif /* UTILS_H */

