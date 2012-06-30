/*
 * visitor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef VISITOR_WRAP_H
#define VISITOR_WRAP_H

#include <node.h>

using namespace node;
using namespace v8;


class VisitorWrap : public ObjectWrap {
  public:
    static void Init(Handle<Object> target);
  private:
    VisitorWrap();
    ~VisitorWrap();

    static Handle<Value> New(const Arguments &args);
};

#endif /* VISITOR_WRAP_H */
