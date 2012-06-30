/*
 * error wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef ERROR_WRAP_H
#define ERROR_WRAP_H

#include <node.h>

using namespace node;
using namespace v8;


class ErrorWrap : public ObjectWrap {
  public:
    static void Init(Handle<Object> target);
  private:
    ErrorWrap();
    ~ErrorWrap();

    static Handle<Value> New(const Arguments &args);
};

#endif /* ERROR_WRAP_H */
