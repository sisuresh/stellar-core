// Copyright 2025 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "invariant/EventsAreConsistentWithEntryDiffs.h"
#include "crypto/SHA.h"
#include "invariant/InvariantManager.h"
#include "ledger/LedgerTxn.h"
#include "main/Application.h"
#include "util/GlobalChecks.h"
#include <fmt/format.h>
#include <numeric>

/*
We're adding the signed 128 bit integer version of lib/util/uint128_t.h here to
prevent exposing it to the protocol. uint128_t.h has deviated from the version
in the repo mentioned which is why this is kept separate.

An signed 128 bit integer type for C++

https://github.com/zhanhb/int128

MIT License

Copyright (c) 2018 zhanhb

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Minor modifications
Copyright (c) 2025 Stellar Development Foundation and contributors.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

*/

#include <cinttypes>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <limits>
#include <locale>
#include <string>
#include <type_traits>
#include "rust/RustBridge.h"

#ifndef __BYTE_ORDER__
#error __BYTE_ORDER__ not defined
#endif

namespace large_int
{
template <class, class> class int128_base;

typedef int128_base<int64_t, uint64_t> int128_t;

template <class _Tp>
struct half_mask
    : std::integral_constant<_Tp, (_Tp(1) << (4 * sizeof(_Tp))) - _Tp(1)>
{
};

template <bool = true> struct detail_delegate;

constexpr bool operator<(int128_t, int128_t);

constexpr int128_t operator>>(int128_t, int);

constexpr int128_t operator*(int128_t, int128_t);

constexpr int128_t operator<<(int128_t, int);

inline int128_t operator/(int128_t, int128_t);

inline int128_t operator%(int128_t, int128_t);

template <class _Hi, class _Low>
class alignas(sizeof(_Hi) * 2) int128_base final
{
    static_assert(sizeof(_Hi) == sizeof(_Low),
                  "low type, high type should have same size");

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    _Low low_{};
    _Hi high_{};

    constexpr int128_base(_Hi high, _Low low) : low_(low), high_(high)
    {
    }

#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    _Hi high_{};
    _Low low_{};

    constexpr int128_base(_Hi high, _Low low) : high_(high), low_(low)
    {
    }

#else
#error endian not support
#endif

    struct integral_tag
    {
    };
    struct signed_integral_tag : integral_tag
    {
    };
    struct unsigned_integral_tag : integral_tag
    {
    };
    struct float_tag
    {
    };
    template <size_t> struct size_constant
    {
    };

  private:
    template <class _Tp>
    constexpr int128_base(_Tp value_, signed_integral_tag, size_constant<8>)
        : int128_base(-(value_ < 0), value_)
    {
    }

    template <class _Tp>
    constexpr int128_base(_Tp value_, unsigned_integral_tag, size_constant<8>)
        : int128_base(0, _Low(value_))
    {
    }

    template <class _Tp>
    constexpr int128_base(_Tp value_, integral_tag, size_constant<16>)
        : // NOLINT explicit
        int128_base(_Hi(value_ >> 64U), _Low(value_))
    {
    } // NOLINT signed shift

  public:
    constexpr int128_base() noexcept = default;

    constexpr int128_base(const int128_base&) noexcept = default;

    constexpr int128_base(int128_base&&) noexcept = default;

    int128_base& operator=(const int128_base&) noexcept = default;

    int128_base& operator=(int128_base&&) noexcept = default;

    template <class _Tp>
    constexpr explicit int128_base(int128_base<_Tp, _Low> val_)
        : int128_base(val_.high_, val_.low_)
    {
    }

    template <class _Tp>
    constexpr int128_base(_Tp val_, float_tag)
        : int128_base(_Hi(std::ldexp(val_, -64)) - (val_ < 0), _Low(val_))
    {
    }

    constexpr explicit int128_base(float val_) : int128_base(val_, float_tag())
    {
    }

    constexpr explicit int128_base(double val_) : int128_base(val_, float_tag())
    {
    }

    constexpr explicit int128_base(long double val_)
        : int128_base(val_, float_tag())
    {
    }

    constexpr int128_base(long long val_)
        : // NOLINT explicit
        int128_base(val_, signed_integral_tag(), size_constant<sizeof(val_)>())
    {
    }

    constexpr int128_base(long val_) : int128_base(static_cast<long long>(val_))
    {
    } // NOLINT explicit

    constexpr int128_base(int val_) : int128_base(long(val_))
    {
    } // NOLINT explicit

    constexpr int128_base(unsigned long long val_)
        : // NOLINT explicit
        int128_base(val_, unsigned_integral_tag(),
                    size_constant<sizeof(val_)>())
    {
    }

    constexpr int128_base(unsigned long val_)
        : // NOLINT explicit
        int128_base(static_cast<unsigned long long>(val_))
    {
    }

    constexpr int128_base(unsigned val_)
        : int128_base(static_cast<unsigned long>(val_))
    {
    } // NOLINT explicit

    constexpr explicit operator bool() const
    {
        return high_ || low_;
    }

    constexpr explicit operator char() const
    {
        return char(low_);
    }

    constexpr explicit operator signed char() const
    {
        return static_cast<signed char>(low_);
    }

    constexpr explicit operator unsigned char() const
    {
        return static_cast<unsigned char>(low_);
    }

    constexpr explicit operator short() const
    {
        return short(low_);
    }

    constexpr explicit operator unsigned short() const
    {
        return static_cast<unsigned short>(low_);
    }

    constexpr explicit operator int() const
    {
        return int(low_);
    }

    constexpr explicit operator unsigned() const
    {
        return unsigned(low_);
    }

    constexpr explicit operator long() const
    {
        return long(low_);
    }

    constexpr explicit operator unsigned long() const
    {
        return static_cast<unsigned long>(low_);
    }

    constexpr explicit operator long long() const
    {
        return static_cast<long long>(low_);
    }

    constexpr explicit operator unsigned long long() const
    {
        return static_cast<unsigned long long>(low_);
    }

    constexpr explicit operator wchar_t() const
    {
        return wchar_t(low_);
    }

    constexpr explicit operator char16_t() const
    {
        return char16_t(low_);
    }

    constexpr explicit operator char32_t() const
    {
        return char32_t(low_);
    }

#if __SIZEOF_INT128__ == 16

    constexpr explicit int128_base(__int128 val_)
        : int128_base(val_, signed_integral_tag(),
                      size_constant<sizeof(val_)>())
    {
    }

    constexpr explicit int128_base(unsigned __int128 val_)
        : int128_base(val_, unsigned_integral_tag(),
                      size_constant<sizeof(val_)>())
    {
    }

    constexpr explicit operator unsigned __int128() const
    {
        return static_cast<unsigned __int128>(high_) << 64U |
               static_cast<unsigned __int128>(low_);
    }

    constexpr explicit operator __int128() const
    {
        return static_cast<__int128>(static_cast<unsigned __int128>(*this));
    }

#endif

  public:
    constexpr int128_base
    operator+() const
    {
        return *this;
    }

    constexpr int128_base
    operator-() const
    {
        return int128_base(-high_ - (low_ != 0), -low_);
    }

    constexpr int128_base
    operator~() const
    {
        return int128_base(~high_, ~low_);
    }

    constexpr bool
    operator!() const
    {
        return !high_ && !low_;
    }

    // avoid self plus on rvalue
    int128_base&
    operator++() &
    {
        return *this = *this + int128_base(1);
    }

    int128_base&
    operator--() &
    {
        return *this = *this - int128_base(1);
    }

    int128_base
    operator++(int) &
    { // NOLINT returns non constant
        int128_base tmp = *this;
        ++*this;
        return tmp;
    }

    int128_base
    operator--(int) &
    { // NOLINT returns non constant
        int128_base tmp = *this;
        --*this;
        return tmp;
    }

    friend constexpr int128_base
    operator+(int128_base lhs_, int128_base rhs_)
    {
        // no worry for unsigned type, won't be optimized if overflow
        return {
            _Hi(lhs_.high_ + rhs_.high_ + (lhs_.low_ + rhs_.low_ < lhs_.low_)),
            lhs_.low_ + rhs_.low_};
    }

    friend constexpr int128_base
    operator-(int128_base lhs_, int128_base rhs_)
    {
        return {_Hi(lhs_.high_ - rhs_.high_ - (lhs_.low_ < rhs_.low_)),
                lhs_.low_ - rhs_.low_};
    }

    friend constexpr int128_base
    operator&(int128_base lhs_, int128_base rhs_)
    {
        return {lhs_.high_ & rhs_.high_, lhs_.low_ & rhs_.low_};
    }

    friend constexpr int128_base
    operator|(int128_base lhs_, int128_base rhs_)
    {
        return {lhs_.high_ | rhs_.high_, lhs_.low_ | rhs_.low_};
    }

    friend constexpr int128_base
    operator^(int128_base lhs_, int128_base rhs_)
    {
        return {lhs_.high_ ^ rhs_.high_, lhs_.low_ ^ rhs_.low_};
    }

    friend constexpr bool
    operator==(int128_base lhs_, int128_base rhs_)
    {
        return lhs_.high_ == rhs_.high_ && lhs_.low_ == rhs_.low_;
    }

    friend constexpr bool
    operator>(int128_base lhs_, int128_base rhs_)
    {
        return rhs_ < lhs_;
    }

    friend constexpr bool
    operator>=(int128_base lhs_, int128_base rhs_)
    {
        return !(lhs_ < rhs_);
    }

    friend constexpr bool
    operator<=(int128_base lhs_, int128_base rhs_)
    {
        return !(rhs_ < lhs_);
    }

    friend constexpr bool
    operator!=(int128_base lhs_, int128_base rhs_)
    {
        return !(lhs_ == rhs_);
    }

    friend constexpr int128_base
    operator<<(int128_base lhs_, int128_base rhs_)
    {
        return lhs_ << (int)rhs_.low_;
    }

    friend constexpr int128_base
    operator>>(int128_base lhs_, int128_base rhs_)
    {
        return lhs_ >> (int)rhs_.low_;
    }

    int128_base&
    operator+=(int128_base rhs_) &
    {
        return *this = *this + rhs_;
    }

    int128_base&
    operator-=(int128_base rhs_) &
    {
        return *this = *this - rhs_;
    }

    int128_base&
    operator*=(int128_base rhs_) &
    {
        return *this = *this * rhs_;
    }

    int128_base&
    operator/=(int128_base rhs_) &
    {
        return *this = *this / rhs_;
    }

    int128_base&
    operator%=(int128_base rhs_) &
    {
        return *this = *this % rhs_;
    }

    int128_base&
    operator<<=(int128_base rhs_) &
    {
        return *this = *this << rhs_;
    }

    int128_base&
    operator>>=(int128_base rhs_) &
    {
        return *this = *this >> rhs_;
    }

    int128_base&
    operator<<=(int rhs_) &
    {
        return *this = *this << rhs_;
    }

    int128_base&
    operator>>=(int rhs_) &
    {
        return *this = *this >> rhs_;
    }

    int128_base&
    operator&=(int128_base rhs_) &
    {
        return *this = *this & rhs_;
    }

    int128_base&
    operator|=(int128_base rhs_) &
    {
        return *this = *this | rhs_;
    }

    int128_base&
    operator^=(int128_base rhs_) &
    {
        return *this = *this ^ rhs_;
    }

    template <class, class> friend class int128_base;

    template <class> friend struct clz_helper;

    template <bool> friend struct detail_delegate;
};

inline namespace literals
{
namespace impl_
{
template <char _Ch, int _Rad>
struct static_digit
    : std::integral_constant<int, '0' <= _Ch && _Ch <= '9'   ? _Ch - '0'
                                  : 'a' <= _Ch && _Ch <= 'z' ? _Ch - 'a' + 10
                                  : 'A' <= _Ch && _Ch <= 'Z' ? _Ch - 'A' + 10
                                                             : _Rad>
{
    static_assert(_Rad > static_digit::value, "character not a digit");
};

template <class, int, char...> struct int128_literal_radix;

template <class _Tp, int _Rad, char _Ch>
struct int128_literal_radix<_Tp, _Rad, _Ch>
{
    constexpr operator _Tp() const
    {
        return _Tp(static_digit<_Ch, _Rad>::value);
    } // NOLINT explicit

    constexpr _Tp
    operator()(_Tp v) const
    {
        return v * _Tp(_Rad) + *this;
    }
};

template <class _Tp, int _Rad, char _Ch, char... _Args>
struct int128_literal_radix<_Tp, _Rad, _Ch, _Args...>
{
    int128_literal_radix<_Tp, _Rad, _Ch> _Cur;
    int128_literal_radix<_Tp, _Rad, _Args...> _Tgt;

    constexpr operator _Tp() const
    {
        return _Tgt(_Cur);
    }; // NOLINT explicit

    constexpr _Tp
    operator()(_Tp v) const
    {
        return _Tgt(_Cur(v));
    };
};

template <class _Tp, char... _Args>
struct int128_literal : int128_literal_radix<_Tp, 10, _Args...>
{
};
template <class _Tp>
struct int128_literal<_Tp, '0'> : int128_literal_radix<_Tp, 10, '0'>
{
};
template <class _Tp, char... _Args>
struct int128_literal<_Tp, '0', _Args...>
    : int128_literal_radix<_Tp, 8, _Args...>
{
};
template <class _Tp, char... _Args>
struct int128_literal<_Tp, '0', 'x', _Args...>
    : int128_literal_radix<_Tp, 16, _Args...>
{
};
template <class _Tp, char... _Args>
struct int128_literal<_Tp, '0', 'X', _Args...>
    : int128_literal_radix<_Tp, 16, _Args...>
{
};
template <class _Tp, char... _Args>
struct int128_literal<_Tp, '0', 'b', _Args...>
    : int128_literal_radix<_Tp, 2, _Args...>
{
};
template <class _Tp, char... _Args>
struct int128_literal<_Tp, '0', 'B', _Args...>
    : int128_literal_radix<_Tp, 2, _Args...>
{
};
}

template <char... _Args> constexpr int128_t operator"" _l128()
{
    return impl_::int128_literal<int128_t, _Args...>();
}

template <char... _Args> constexpr int128_t operator"" _L128()
{
    return impl_::int128_literal<int128_t, _Args...>();
}
}

template <class> struct clz_helper;

template <> struct clz_helper<unsigned long>
{
    static constexpr int
    clz(unsigned long val_)
    {
        return __builtin_clzl(val_);
    }
};

template <> struct clz_helper<unsigned long long>
{
    static constexpr int
    clz(unsigned long long val_)
    {
        return __builtin_clzll(val_);
    }
};

template <class _High, class _Low> struct clz_helper<int128_base<_High, _Low>>
{
    static constexpr int
    clz(int128_base<_High, _Low> val_)
    {
        return val_.high_ ? clz_helper<_Low>::clz(val_.high_)
                          : 4 * sizeof(val_) + clz_helper<_Low>::clz(val_.low_);
    }
};

template <bool> struct detail_delegate
{
    template <class _Hi, class _Low>
    static constexpr bool
    cmp(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return lhs_.high_ < rhs_.high_ ||
               (lhs_.high_ == rhs_.high_ && lhs_.low_ < rhs_.low_);
    }

    static constexpr int128_t
    sar(int128_t lhs_, unsigned rhs_)
    {
        return rhs_ & 64U ? int128_t(-(lhs_.high_ < 0),
                                     uint64_t(lhs_.high_ >> (rhs_ & 63U)))
                          : // NOLINT
                   rhs_ & 63U
                   ? int128_t(lhs_.high_ >> (rhs_ & 63U), // NOLINT signed shift
                              (uint64_t(lhs_.high_) << (64 - (rhs_ & 63U)) |
                               (lhs_.low_ >> (rhs_ & 63U))))
                   : lhs_;
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    imul(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return int128_base<_Hi, _Low>(
                   _Hi(lhs_.low_ * rhs_.high_ + rhs_.low_ * lhs_.high_) +
                       (lhs_.low_ >> 32U) * (rhs_.low_ >> 32U),
                   (lhs_.low_ & half_mask<_Low>::value) *
                       (rhs_.low_ & half_mask<_Low>::value)) +
               (int128_base<_Hi, _Low>((lhs_.low_ >> 32U) *
                                       (rhs_.low_ & half_mask<_Low>::value))
                << 32U) +
               (int128_base<_Hi, _Low>((rhs_.low_ >> 32U) *
                                       (lhs_.low_ & half_mask<_Low>::value))
                << 32U);
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    shl(int128_base<_Hi, _Low> lhs_, unsigned rhs_)
    {
        // [64,127], 64 {low_ << 0, 0}
        return rhs_ & 64U ? int128_base<_Hi, _Low>(
                                _Hi(lhs_.low_ << (rhs_ & 63U)), _Low(0))
               : rhs_ & 63U ? int128_base<_Hi, _Low>(
                                  _Hi((_Low(lhs_.high_) << (rhs_ & 63U)) |
                                      (lhs_.low_ >> (64U - (rhs_ & 63U)))),
                                  lhs_.low_ << (rhs_ & 63U))
                            : lhs_;
    }
};

#if __SIZEOF_INT128__ == 16

template <> struct detail_delegate<true>
{
    typedef __int128 ti_int_;
    typedef unsigned __int128 tu_int_;

    static constexpr ti_int_
    to_native(int128_t val_)
    {
        return static_cast<ti_int_>(val_);
    }

    static constexpr int128_t
    from_native(ti_int_ val_)
    {
        return int128_t(val_);
    }

    template <class _Hi, class _Low>
    static constexpr bool
    cmp(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return to_native(lhs_) < to_native(rhs_);
    }

    static constexpr int128_t
    sar(int128_t lhs_, unsigned rhs_)
    {
        return from_native(to_native(lhs_) >>
                           static_cast<decltype(to_native(lhs_))>(
                               rhs_)); // NOLINT signed shift
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    imul(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return from_native(to_native(lhs_) * to_native(rhs_));
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    shl(int128_base<_Hi, _Low> lhs_, unsigned rhs_)
    {
        return from_native(to_native(lhs_)
                           << static_cast<decltype(to_native(lhs_))>(
                                  rhs_)); // NOLINT signed shift
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    div(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return from_native(to_native(lhs_) / to_native(rhs_));
    }

    template <class _Hi, class _Low>
    static constexpr int128_base<_Hi, _Low>
    mod(int128_base<_Hi, _Low> lhs_, int128_base<_Hi, _Low> rhs_)
    {
        return from_native(to_native(lhs_) % to_native(rhs_));
    }
};

#endif

constexpr bool
operator<(int128_t lhs_, int128_t rhs_)
{
    return detail_delegate<>::cmp(lhs_, rhs_);
}

constexpr int128_t
operator>>(int128_t lhs_, int rhs_)
{
    return detail_delegate<>::sar(lhs_, static_cast<unsigned>(rhs_));
}

constexpr int128_t
operator*(int128_t lhs_, int128_t rhs_)
{
    return detail_delegate<>::imul(lhs_, rhs_);
}

constexpr int128_t
operator<<(int128_t lhs_, int rhs_)
{
    return detail_delegate<>::shl(lhs_, static_cast<unsigned>(rhs_));
}

inline int128_t
operator/(int128_t lhs_, int128_t rhs_)
{
    return detail_delegate<>::div(lhs_, rhs_);
};

inline int128_t
operator%(int128_t lhs_, int128_t rhs_)
{
    return detail_delegate<>::mod(lhs_, rhs_);
}
}

#ifndef INT128_NO_EXPORT
#define INT128_C(val) val##_L128
// add space between ‘""’ and suffix identifier, or may compile failed
using namespace large_int::literals;
using large_int::int128_t;
#endif /* INT128_NO_EXPORT */

namespace stellar
{

static const CxxI128 I128ZERO{0, 0};

struct AggregatedEvents
{
    mutable UnorderedMap<LedgerKey, UnorderedMap<Asset, CxxI128>>
        mEventAmounts;
    UnorderedMap<Hash, Asset> mStellarAssetContractIDs;

    void addAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount);

    void subtractAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount);
};

void AggregatedEvents::addAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount) {
    mEventAmounts[lk][asset] = rust_bridge::i128_add(mEventAmounts[lk][asset], amount);
}

void AggregatedEvents::subtractAssetBalance(LedgerKey const& lk, Asset const& asset, CxxI128 const& amount) {
    mEventAmounts[lk][asset] = rust_bridge::i128_sub(mEventAmounts[lk][asset], amount);
}

static CxxI128
getAmountFromData(SCVal const& data)
{
    if (data.type() == SCV_I128)
    {
        auto const& amountVal = data.i128();
        return CxxI128{amountVal.hi, amountVal.lo};
    }
    else if (data.type() == SCV_MAP && data.map())
    {
        auto const& map = *data.map();
        for (auto const& entry : map)
        {
            if (entry.key.type() == SCV_SYMBOL && entry.key.sym() == "amount" &&
                entry.val.type() == SCV_I128)
            {
                auto const& amountVal = entry.val.i128();
                return CxxI128{amountVal.hi, amountVal.lo};
            }
        }
    }
    return I128ZERO;
}

static CxxI128
consumeAmount(
    LedgerKey const& lk, Asset const& asset,
    UnorderedMap<LedgerKey, UnorderedMap<Asset, CxxI128>>& eventAmounts)
{
    // If the key is ContractData, then it is a balance entry. We need to
    // convert it to the ledger key for the address that the balance belongs
    // too.
    LedgerKey ledgerKeyToLookup;
    if (lk.type() == CONTRACT_DATA)
    {
        if (lk.contractData().key.type() != SCV_VEC)
        {
            return I128ZERO;
        }

        auto const& vec = lk.contractData().key.vec();
        if (!vec || vec->size() != 2)
        {
            return I128ZERO;
        }

        auto const& addr = vec->at(1);

        if (addr.type() != SCV_ADDRESS)
        {
            return I128ZERO;
        }

        ledgerKeyToLookup = addressToLedgerKey(addr.address());
    }
    else
    {
        ledgerKeyToLookup = lk;
    }

    auto lkAssetMapIt = eventAmounts.find(ledgerKeyToLookup);
    if (lkAssetMapIt == eventAmounts.end())
    {
        return I128ZERO;
    }

    auto& lkAssetMap = lkAssetMapIt->second;
    auto assetAmountIt = lkAssetMap.find(asset);
    if (assetAmountIt == lkAssetMap.end())
    {
        return I128ZERO;
    }

    auto res = assetAmountIt->second;

    // Now remove this value from the map
    lkAssetMap.erase(assetAmountIt);
    if (lkAssetMap.empty())
    {
        eventAmounts.erase(lkAssetMapIt);
    }
    return res;
}

static std::string
calculateDeltaBalance(AggregatedEvents const& agg, LedgerEntry const* current,
                      LedgerEntry const* previous)
{
    releaseAssert(current || previous);
    auto lk = current ? LedgerEntryKey(*current) : LedgerEntryKey(*previous);
    auto let = current ? current->data.type() : previous->data.type();

    switch (let)
    {
    case ACCOUNT:
    {
        Asset native(ASSET_TYPE_NATIVE);
        auto eventDiff = consumeAmount(lk, native, agg.mEventAmounts);

        auto entryDiff = (current ? current->data.account().balance : 0) -
                         (previous ? previous->data.account().balance : 0);

        return entryDiff == (int64)eventDiff.lo ? ""
                                      : "Account diff does not match events";
    }
    case TRUSTLINE:
    {
        auto const& tlAsset = current ? current->data.trustLine().asset
                                      : previous->data.trustLine().asset;

        Asset asset;
        switch (tlAsset.type())
        {
        case stellar::ASSET_TYPE_CREDIT_ALPHANUM4:
            asset.type(ASSET_TYPE_CREDIT_ALPHANUM4);
            asset.alphaNum4() = tlAsset.alphaNum4();
            break;
        case stellar::ASSET_TYPE_CREDIT_ALPHANUM12:
            asset.type(ASSET_TYPE_CREDIT_ALPHANUM12);
            asset.alphaNum12() = tlAsset.alphaNum12();
            break;
        case stellar::ASSET_TYPE_POOL_SHARE:
            return "";
        case stellar::ASSET_TYPE_NATIVE:
        default:
            return "Invalid asset in trustline";
        }

        auto const& trustlineOwner = current
                                         ? current->data.trustLine().accountID
                                         : previous->data.trustLine().accountID;

        LedgerKey accKey(ACCOUNT);
        accKey.account().accountID = trustlineOwner;
        auto eventDiff = consumeAmount(accKey, asset, agg.mEventAmounts);

        auto entryDiff = (current ? current->data.trustLine().balance : 0) -
                         (previous ? previous->data.trustLine().balance : 0);

        return entryDiff == (int64) eventDiff.lo ? ""
                                      : "Trustline diff does not match events";
    }
    case OFFER:
        break;
    case DATA:
        break;
    case CLAIMABLE_BALANCE:
    {
        auto const& asset = current ? current->data.claimableBalance().asset
                                    : previous->data.claimableBalance().asset;

        auto eventDiff = consumeAmount(lk, asset, agg.mEventAmounts);
        auto entryDiff =
            (current ? current->data.claimableBalance().amount : 0) -
            (previous ? previous->data.claimableBalance().amount : 0);

        return entryDiff == (int64) eventDiff.lo
                   ? ""
                   : "ClaimableBalance diff does not match events";
    }
    case LIQUIDITY_POOL:
    {
        auto const* currentBody =
            current ? &current->data.liquidityPool().body.constantProduct()
                    : nullptr;
        auto const* previousBody =
            previous ? &previous->data.liquidityPool().body.constantProduct()
                     : nullptr;

        auto const& assetA =
            (currentBody ? currentBody : previousBody)->params.assetA;
        auto const& assetB =
            (currentBody ? currentBody : previousBody)->params.assetB;

        int64_t entryADiff = (currentBody ? currentBody->reserveA : 0) -
                             (previousBody ? previousBody->reserveA : 0);

        int64_t entryBDiff = (currentBody ? currentBody->reserveB : 0) -
                             (previousBody ? previousBody->reserveB : 0);

        auto eventADiff = consumeAmount(lk, assetA, agg.mEventAmounts);
        auto eventBDiff = consumeAmount(lk, assetB, agg.mEventAmounts);

        return entryADiff == (int64) eventADiff.lo && entryBDiff == (int64) eventBDiff.lo
                   ? ""
                   : "LiquidityPool diff does not match events";
    }
    case CONTRACT_DATA:
    {
        auto const& contractData = current ? current->data.contractData()
                                           : previous->data.contractData();

        if (contractData.contract.type() != SC_ADDRESS_TYPE_CONTRACT)
        {
            return "";
        }

        auto assetIt = agg.mStellarAssetContractIDs.find(
            contractData.contract.contractId());
        if (assetIt == agg.mStellarAssetContractIDs.end())
        {
            return "";
        }

        auto const& asset = assetIt->second;

        auto getAmount = [](LedgerEntry const* entry) -> CxxI128 {
            if (!entry)
            {
                return I128ZERO;
            }

            // Make sure this is the balance entry and not an allowance
            auto const& dataKey = entry->data.contractData().key;
            if (dataKey.type() != SCV_VEC || !dataKey.vec() ||
                dataKey.vec()->size() != 2)
            {
                return I128ZERO;
            }
            auto const& name = dataKey.vec()->at(0);
            if (name.type() != SCV_SYMBOL || name.sym() != "Balance")
            {
                return I128ZERO;
            }

            // The amount should be the first entry in the SCMap
            auto const& val = entry->data.contractData().val;
            if (val.type() == SCV_MAP && val.map() && val.map()->size() != 0)
            {
                auto const& amountEntry = val.map()->at(0);
                return getAmountFromData(amountEntry.val);
            }
            return I128ZERO;
        };

        auto eventDiff = consumeAmount(lk, asset, agg.mEventAmounts);
        auto entryDiff = rust_bridge::i128_sub(getAmount(current), getAmount(previous));
        return entryDiff == eventDiff
                   ? ""
                   : "ContractData diff does not match events";
    }
    case CONTRACT_CODE:
        break;
    case CONFIG_SETTING:
        break;
    case TTL:
        break;
    }
    return "";
}

static std::string
verifyEventsDelta(AggregatedEvents const& agg,
                  std::shared_ptr<InternalLedgerEntry const> const& genCurrent,
                  std::shared_ptr<InternalLedgerEntry const> const& genPrevious)
{
    auto type = genCurrent ? genCurrent->type() : genPrevious->type();
    if (type == InternalLedgerEntryType::LEDGER_ENTRY)
    {
        auto const* current = genCurrent ? &genCurrent->ledgerEntry() : nullptr;
        auto const* previous =
            genPrevious ? &genPrevious->ledgerEntry() : nullptr;

        return calculateDeltaBalance(agg, current, previous);
    }
    return "";
}

static AggregatedEvents
aggregateEventDiffs(Hash const& networkID,
                    std::vector<ContractEvent> const& events)
{
    AggregatedEvents res;
    for (auto const& event : events)
    {
        if (!event.contractID)
        {
            continue;
        }
        auto const& topics = event.body.v0().topics;
        Asset asset;
        auto assetIt = res.mStellarAssetContractIDs.find(*event.contractID);
        if (assetIt != res.mStellarAssetContractIDs.end())
        {
            asset = assetIt->second;
        }
        else
        {
            auto maybeAsset = isFromSAC(event, networkID);
            if (maybeAsset)
            {
                asset = *maybeAsset;
                res.mStellarAssetContractIDs.emplace(*event.contractID, asset);
            }
            else
            {
                continue;
            }
        }
        // at this point, we have verified that this is an SAC event

        auto eventNameVal = topics.at(0);
        if (eventNameVal.type() != SCV_SYMBOL)
        {
            continue;
        }
        if (eventNameVal.sym() == "transfer")
        {
            if (topics.size() != 4)
            {
                continue;
            }

            auto const& fromVal = topics.at(1);
            auto const& toVal = topics.at(2);

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto toLk = addressToLedgerKey(toVal.address());

            auto amount = getAmountFromData(event.body.v0().data);

            // If the events are sane, we should never overflow.
            // TODO: Handle the overflow case anyways?
            res.subtractAssetBalance(fromLk, asset, amount);
            res.addAssetBalance(toLk, asset, amount);
        }
        else if (eventNameVal.sym() == "mint")
        {
            if (topics.size() != 3)
            {
                continue;
            }

            auto toVal = topics.at(1);

            auto toLk = addressToLedgerKey(toVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.addAssetBalance(toLk, asset, amount);
        }
        else if (eventNameVal.sym() == "burn" ||
                 eventNameVal.sym() == "clawback")
        {
            if (topics.size() != 3)
            {
                continue;
            }

            auto fromVal = topics.at(1);

            auto fromLk = addressToLedgerKey(fromVal.address());
            auto amount = getAmountFromData(event.body.v0().data);
            res.subtractAssetBalance(fromLk, asset, amount);
        }
    }
    return res;
}

EventsAreConsistentWithEntryDiffs::EventsAreConsistentWithEntryDiffs(
    Hash const& networkID)
    : Invariant(false), mNetworkID(networkID)
{
}

std::shared_ptr<Invariant>
EventsAreConsistentWithEntryDiffs::registerInvariant(Application& app)
{
    return app.getInvariantManager()
        .registerInvariant<EventsAreConsistentWithEntryDiffs>(
            app.getNetworkID());
}

std::string
EventsAreConsistentWithEntryDiffs::getName() const
{
    return "EventsAreConsistentWithEntryDiffs";
}

// Note that this invariant only verifies balance changes in the context of an
// operation. The fee events should not be accounted for here.
std::string
EventsAreConsistentWithEntryDiffs::checkOnOperationApply(
    Operation const& operation, OperationResult const& result,
    LedgerTxnDelta const& ltxDelta, std::vector<ContractEvent> const& events)
{
    auto aggregatedEventAmounts = aggregateEventDiffs(mNetworkID, events);
    for (auto const& delta : ltxDelta.entry)
    {
        auto res =
            verifyEventsDelta(aggregatedEventAmounts, delta.second.current,
                              delta.second.previous);
        if (!res.empty())
        {
            return res;
        }
    }

    for (auto const& kvp : aggregatedEventAmounts.mEventAmounts)
    {
        for (auto const& kvp2 : kvp.second)
        {
            if (kvp2.second != I128ZERO)
            {
                return "Some event diffs not consumed";
            }
        }
    }
    return {};
}
}