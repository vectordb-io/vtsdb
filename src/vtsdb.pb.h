// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: vtsdb.proto

#ifndef GOOGLE_PROTOBUF_INCLUDED_vtsdb_2eproto
#define GOOGLE_PROTOBUF_INCLUDED_vtsdb_2eproto

#include <limits>
#include <string>

#include <google/protobuf/port_def.inc>
#if PROTOBUF_VERSION < 3008000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers. Please update
#error your headers.
#endif
#if 3008000 < PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers. Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/port_undef.inc>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/arena.h>
#include <google/protobuf/arenastring.h>
#include <google/protobuf/generated_message_table_driven.h>
#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/inlined_string_field.h>
#include <google/protobuf/metadata.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>  // IWYU pragma: export
#include <google/protobuf/extension_set.h>  // IWYU pragma: export
#include <google/protobuf/unknown_field_set.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>
#define PROTOBUF_INTERNAL_EXPORT_vtsdb_2eproto
PROTOBUF_NAMESPACE_OPEN
namespace internal {
class AnyMetadata;
}  // namespace internal
PROTOBUF_NAMESPACE_CLOSE

// Internal implementation detail -- do not use these members.
struct TableStruct_vtsdb_2eproto {
  static const ::PROTOBUF_NAMESPACE_ID::internal::ParseTableField entries[]
    PROTOBUF_SECTION_VARIABLE(protodesc_cold);
  static const ::PROTOBUF_NAMESPACE_ID::internal::AuxillaryParseTableField aux[]
    PROTOBUF_SECTION_VARIABLE(protodesc_cold);
  static const ::PROTOBUF_NAMESPACE_ID::internal::ParseTable schema[1]
    PROTOBUF_SECTION_VARIABLE(protodesc_cold);
  static const ::PROTOBUF_NAMESPACE_ID::internal::FieldMetadata field_metadata[];
  static const ::PROTOBUF_NAMESPACE_ID::internal::SerializationTable serialization_table[];
  static const ::PROTOBUF_NAMESPACE_ID::uint32 offsets[];
};
extern const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable descriptor_table_vtsdb_2eproto;
namespace vtsdb {
class Strings;
class StringsDefaultTypeInternal;
extern StringsDefaultTypeInternal _Strings_default_instance_;
}  // namespace vtsdb
PROTOBUF_NAMESPACE_OPEN
template<> ::vtsdb::Strings* Arena::CreateMaybeMessage<::vtsdb::Strings>(Arena*);
PROTOBUF_NAMESPACE_CLOSE
namespace vtsdb {

// ===================================================================

class Strings :
    public ::PROTOBUF_NAMESPACE_ID::Message /* @@protoc_insertion_point(class_definition:vtsdb.Strings) */ {
 public:
  Strings();
  virtual ~Strings();

  Strings(const Strings& from);
  Strings(Strings&& from) noexcept
    : Strings() {
    *this = ::std::move(from);
  }

  inline Strings& operator=(const Strings& from) {
    CopyFrom(from);
    return *this;
  }
  inline Strings& operator=(Strings&& from) noexcept {
    if (GetArenaNoVirtual() == from.GetArenaNoVirtual()) {
      if (this != &from) InternalSwap(&from);
    } else {
      CopyFrom(from);
    }
    return *this;
  }

  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* descriptor() {
    return GetDescriptor();
  }
  static const ::PROTOBUF_NAMESPACE_ID::Descriptor* GetDescriptor() {
    return GetMetadataStatic().descriptor;
  }
  static const ::PROTOBUF_NAMESPACE_ID::Reflection* GetReflection() {
    return GetMetadataStatic().reflection;
  }
  static const Strings& default_instance();

  static void InitAsDefaultInstance();  // FOR INTERNAL USE ONLY
  static inline const Strings* internal_default_instance() {
    return reinterpret_cast<const Strings*>(
               &_Strings_default_instance_);
  }
  static constexpr int kIndexInFileMessages =
    0;

  void Swap(Strings* other);
  friend void swap(Strings& a, Strings& b) {
    a.Swap(&b);
  }

  // implements Message ----------------------------------------------

  inline Strings* New() const final {
    return CreateMaybeMessage<Strings>(nullptr);
  }

  Strings* New(::PROTOBUF_NAMESPACE_ID::Arena* arena) const final {
    return CreateMaybeMessage<Strings>(arena);
  }
  void CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) final;
  void MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) final;
  void CopyFrom(const Strings& from);
  void MergeFrom(const Strings& from);
  PROTOBUF_ATTRIBUTE_REINITIALIZES void Clear() final;
  bool IsInitialized() const final;

  size_t ByteSizeLong() const final;
  #if GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
  const char* _InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) final;
  #else
  bool MergePartialFromCodedStream(
      ::PROTOBUF_NAMESPACE_ID::io::CodedInputStream* input) final;
  #endif  // GOOGLE_PROTOBUF_ENABLE_EXPERIMENTAL_PARSER
  void SerializeWithCachedSizes(
      ::PROTOBUF_NAMESPACE_ID::io::CodedOutputStream* output) const final;
  ::PROTOBUF_NAMESPACE_ID::uint8* InternalSerializeWithCachedSizesToArray(
      ::PROTOBUF_NAMESPACE_ID::uint8* target) const final;
  int GetCachedSize() const final { return _cached_size_.Get(); }

  private:
  inline void SharedCtor();
  inline void SharedDtor();
  void SetCachedSize(int size) const final;
  void InternalSwap(Strings* other);
  friend class ::PROTOBUF_NAMESPACE_ID::internal::AnyMetadata;
  static ::PROTOBUF_NAMESPACE_ID::StringPiece FullMessageName() {
    return "vtsdb.Strings";
  }
  private:
  inline ::PROTOBUF_NAMESPACE_ID::Arena* GetArenaNoVirtual() const {
    return nullptr;
  }
  inline void* MaybeArenaPtr() const {
    return nullptr;
  }
  public:

  ::PROTOBUF_NAMESPACE_ID::Metadata GetMetadata() const final;
  private:
  static ::PROTOBUF_NAMESPACE_ID::Metadata GetMetadataStatic() {
    ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(&::descriptor_table_vtsdb_2eproto);
    return ::descriptor_table_vtsdb_2eproto.file_level_metadata[kIndexInFileMessages];
  }

  public:

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // repeated string strings = 1;
  int strings_size() const;
  void clear_strings();
  static const int kStringsFieldNumber = 1;
  const std::string& strings(int index) const;
  std::string* mutable_strings(int index);
  void set_strings(int index, const std::string& value);
  void set_strings(int index, std::string&& value);
  void set_strings(int index, const char* value);
  void set_strings(int index, const char* value, size_t size);
  std::string* add_strings();
  void add_strings(const std::string& value);
  void add_strings(std::string&& value);
  void add_strings(const char* value);
  void add_strings(const char* value, size_t size);
  const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<std::string>& strings() const;
  ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<std::string>* mutable_strings();

  // @@protoc_insertion_point(class_scope:vtsdb.Strings)
 private:
  class HasBitSetters;

  ::PROTOBUF_NAMESPACE_ID::internal::InternalMetadataWithArena _internal_metadata_;
  ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<std::string> strings_;
  mutable ::PROTOBUF_NAMESPACE_ID::internal::CachedSize _cached_size_;
  friend struct ::TableStruct_vtsdb_2eproto;
};
// ===================================================================


// ===================================================================

#ifdef __GNUC__
  #pragma GCC diagnostic push
  #pragma GCC diagnostic ignored "-Wstrict-aliasing"
#endif  // __GNUC__
// Strings

// repeated string strings = 1;
inline int Strings::strings_size() const {
  return strings_.size();
}
inline void Strings::clear_strings() {
  strings_.Clear();
}
inline const std::string& Strings::strings(int index) const {
  // @@protoc_insertion_point(field_get:vtsdb.Strings.strings)
  return strings_.Get(index);
}
inline std::string* Strings::mutable_strings(int index) {
  // @@protoc_insertion_point(field_mutable:vtsdb.Strings.strings)
  return strings_.Mutable(index);
}
inline void Strings::set_strings(int index, const std::string& value) {
  // @@protoc_insertion_point(field_set:vtsdb.Strings.strings)
  strings_.Mutable(index)->assign(value);
}
inline void Strings::set_strings(int index, std::string&& value) {
  // @@protoc_insertion_point(field_set:vtsdb.Strings.strings)
  strings_.Mutable(index)->assign(std::move(value));
}
inline void Strings::set_strings(int index, const char* value) {
  GOOGLE_DCHECK(value != nullptr);
  strings_.Mutable(index)->assign(value);
  // @@protoc_insertion_point(field_set_char:vtsdb.Strings.strings)
}
inline void Strings::set_strings(int index, const char* value, size_t size) {
  strings_.Mutable(index)->assign(
    reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_set_pointer:vtsdb.Strings.strings)
}
inline std::string* Strings::add_strings() {
  // @@protoc_insertion_point(field_add_mutable:vtsdb.Strings.strings)
  return strings_.Add();
}
inline void Strings::add_strings(const std::string& value) {
  strings_.Add()->assign(value);
  // @@protoc_insertion_point(field_add:vtsdb.Strings.strings)
}
inline void Strings::add_strings(std::string&& value) {
  strings_.Add(std::move(value));
  // @@protoc_insertion_point(field_add:vtsdb.Strings.strings)
}
inline void Strings::add_strings(const char* value) {
  GOOGLE_DCHECK(value != nullptr);
  strings_.Add()->assign(value);
  // @@protoc_insertion_point(field_add_char:vtsdb.Strings.strings)
}
inline void Strings::add_strings(const char* value, size_t size) {
  strings_.Add()->assign(reinterpret_cast<const char*>(value), size);
  // @@protoc_insertion_point(field_add_pointer:vtsdb.Strings.strings)
}
inline const ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<std::string>&
Strings::strings() const {
  // @@protoc_insertion_point(field_list:vtsdb.Strings.strings)
  return strings_;
}
inline ::PROTOBUF_NAMESPACE_ID::RepeatedPtrField<std::string>*
Strings::mutable_strings() {
  // @@protoc_insertion_point(field_mutable_list:vtsdb.Strings.strings)
  return &strings_;
}

#ifdef __GNUC__
  #pragma GCC diagnostic pop
#endif  // __GNUC__

// @@protoc_insertion_point(namespace_scope)

}  // namespace vtsdb

// @@protoc_insertion_point(global_scope)

#include <google/protobuf/port_undef.inc>
#endif  // GOOGLE_PROTOBUF_INCLUDED_GOOGLE_PROTOBUF_INCLUDED_vtsdb_2eproto
