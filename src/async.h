/*
 * async modules
 * Copyright (C) 2012 kazuya kawaguchi. See Copyright Notice in kyotocabinet.h
 */

#ifndef ASYNC_H
#define ASYNC_H

#include <node.h>
#include <kcpolydb.h>
#include <kcthread.h>
#include <kcdbext.h>


using namespace node;
using namespace v8;
using namespace kyotocabinet;


void kc_async_init(uv_loop_t *loop);


class AsyncVisitor : public PolyDB::Visitor {
  public:
    AsyncVisitor(Persistent<Object> &cb, bool writable);
    ~AsyncVisitor();

  private:
    Persistent<Object> cb_;
    bool writable_;

    const char* visit_full(
      const char *kbuf, size_t ksiz,
      const char *vbuf, size_t vsiz, size_t *sp
    );
    const char* visit_empty(const char *kbuf, size_t ksiz, size_t *sp);
    void visit_before();
    void visit_after();
};

class AsyncFileProcessor : public PolyDB::FileProcessor {
  public:
    AsyncFileProcessor(Persistent<Object> &cb);
    ~AsyncFileProcessor();

  private:
    Persistent<Object> cb_;

    bool process(const std::string &path, int64_t count, int64_t size);
};

#endif /* ASYNC_H */
