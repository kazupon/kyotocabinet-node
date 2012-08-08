/*
 * visitor wapper
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#define BUILDING_NODE_EXTENSION

#include <kcdb.h>
#include "visitor_wrap.h"
#include <assert.h>

using namespace v8;
using namespace kyotocabinet;


VisitorWrap::VisitorWrap() {};
VisitorWrap::~VisitorWrap() {};

Handle<Value> VisitorWrap::New(const Arguments &args) {
  HandleScope scope;

  VisitorWrap *obj = new VisitorWrap();
  obj->Wrap(args.This());

  return args.This();
}


void VisitorWrap::Init(Handle<Object> target) {
  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("Visitor"));

  Local<ObjectTemplate> insttpl = tpl->InstanceTemplate();
  insttpl->SetInternalFieldCount(1);

  // prototype
  Local<ObjectTemplate> prottpl = tpl->PrototypeTemplate();
  //tpl->PrototypeTemplate()->Set(String::NewSymbol("plusOne"), FunctionTemplate::New(PlusOne)->GetFunction());

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("Visitor"), ctor);
  ctor->Set(String::NewSymbol("NOP"), Integer::New(0), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
  ctor->Set(String::NewSymbol("REMOVE"), Integer::New(1), static_cast<PropertyAttribute>(ReadOnly | DontDelete));
}

