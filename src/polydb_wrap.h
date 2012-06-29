/*
 * polydb wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef POLYDB_WRAP_H
#define POLYDB_WRAP_H

#include <node.h>
#include <kcpolydb.h>


using namespace node;
using namespace v8;
using namespace kyotocabinet;


class PolyDBWrap : public ObjectWrap {
  public:
    static void Init(Handle<Object> target);
  private:
    PolyDBWrap();
    ~PolyDBWrap();

    static Handle<Value> New(const Arguments &args);
};

#endif /* POLYDB_WRAP_H */
