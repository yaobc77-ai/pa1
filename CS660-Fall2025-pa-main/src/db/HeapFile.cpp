#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {}

// 插入到“最后一页的第一个空槽”。若最后一页满了，则新建一页写入。
void HeapFile::insertTuple(const Tuple &t) {
    if (!getTupleDesc().compatible(t)) {
        throw std::logic_error("HeapFile::insertTuple: tuple not compatible with schema");
    }

    const TupleDesc &td = getTupleDesc();
    const size_t n = getNumPages();

    if (n > 0) {
        // 试图写入最后一页
        Page page{};
        readPage(page, n - 1);
        HeapPage hp(page, td);
        if (hp.insertTuple(t)) {
            writePage(page, n - 1);
            return;
        }
    }

    // 最后一页不存在或已满 -> 新建空页并写入
    Page new_page{};                // 全 0 即空页
    HeapPage hp_new(new_page, td);
    (void)hp_new.insertTuple(t);    // 首条一定能插入
    writePage(new_page, n);         // 追加为第 n 页（0-based）
}

// 根据迭代器定位并删除槽位（页在范围内由 HeapPage 自行做槽位校验）
void HeapFile::deleteTuple(const Iterator &it) {
    const size_t n = getNumPages();
    if (it.page >= n) throw std::out_of_range("HeapFile::deleteTuple: page out of range");

    Page page{};
    readPage(page, it.page);
    HeapPage hp(page, getTupleDesc());
    hp.deleteTuple(it.slot);
    writePage(page, it.page);
}

// 读取迭代器指定位置的元组
Tuple HeapFile::getTuple(const Iterator &it) const {
    const size_t n = getNumPages();
    if (it.page >= n) throw std::out_of_range("HeapFile::getTuple: page out of range");

    Page page{};
    readPage(page, it.page);
    const HeapPage hp(page, getTupleDesc());
    return hp.getTuple(it.slot);
}

// 将迭代器推进到下一个已占用槽；若到末尾，设为 end() 哨兵
void HeapFile::next(Iterator &it) const {
    const size_t n = getNumPages();
    if (it.page >= n) { // 已经在 end 或越界
        it.page = n;
        it.slot = 0;
        return;
    }

    // 当前页内尝试下一个
    Page page{};
    readPage(page, it.page);
    HeapPage hp(page, getTupleDesc());

    size_t s = it.slot;
    hp.next(s);
    if (s != hp.end()) {   // 仍在当前页找到
        it.slot = s;
        return;
    }

    // 跨页寻找下一非空页
    for (size_t p = it.page + 1; p < n; ++p) {
        Page pg{};
        readPage(pg, p);
        HeapPage hpg(pg, getTupleDesc());
        size_t b = hpg.begin();
        if (b != hpg.end()) {
            it.page = p;
            it.slot = b;
            return;
        }
    }

    // 没有更多元组：设置为 end 哨兵（注意不能做对象赋值，赋值运算符被删除）
    it.page = n;
    it.slot = 0;
}

// 找到文件中的第一个已占用槽位
Iterator HeapFile::begin() const {
    const size_t n = getNumPages();
    for (size_t p = 0; p < n; ++p) {
        Page page{};
        readPage(page, p);
        HeapPage hp(page, getTupleDesc());
        size_t b = hp.begin();
        if (b != hp.end()) {
            return Iterator(*this, p, b);
        }
    }
    // 文件没有任何元组，直接返回 end()
    return Iterator(*this, n, 0);
}

// end 哨兵：page==getNumPages()，slot 任意（用 0）
Iterator HeapFile::end() const {
    return Iterator(*this, getNumPages(), 0);
}
