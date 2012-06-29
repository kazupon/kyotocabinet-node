#define BUILDING_NODE_EXTENSION

#include <node.h>
#include "polydb_wrap.h"
#include "debug.h"
#include <assert.h>

using namespace v8;


PolyDBWrap::PolyDBWrap() {};
PolyDBWrap::~PolyDBWrap() {};

void PolyDBWrap::Init(Handle<Object> target) {
  // prepare constructor template
  Local<FunctionTemplate> tpl = FunctionTemplate::New(New);
  tpl->SetClassName(String::NewSymbol("DB"));
  tpl->InstanceTemplate()->SetInternalFieldCount(1);

  // prototype
  //tpl->PrototypeTemplate()->Set(String::NewSymbol("plusOne"), FunctionTemplate::New(PlusOne)->GetFunction());

  Persistent<Function> ctor = Persistent<Function>::New(tpl->GetFunction());
  target->Set(String::NewSymbol("DB"), ctor);
}

Handle<Value> PolyDBWrap::New(const Arguments &args) {
  HandleScope scope;
  TRACE("polydb_wrap init\n");

  PolyDBWrap *obj = new PolyDBWrap();
  obj->Wrap(args.This());

  return args.This();
}

