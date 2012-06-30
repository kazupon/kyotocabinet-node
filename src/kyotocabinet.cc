/*
 * main
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#include <node.h>
#include "kyotocabinet.h"
#include "error_wrap.h"
#include "visitor_wrap.h"
#include "polydb_wrap.h"

using namespace v8;


void Initialize (Handle<Object> target) {
  ErrorWrap::Init(target);
  VisitorWrap::Init(target);
  PolyDBWrap::Init(target);
}

NODE_MODULE(kyotocabinet, Initialize);
