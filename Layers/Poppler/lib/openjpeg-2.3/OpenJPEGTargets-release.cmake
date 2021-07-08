#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "openjp2" for configuration "RELEASE"
set_property(TARGET openjp2 APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(openjp2 PROPERTIES
  IMPORTED_LINK_INTERFACE_LIBRARIES_RELEASE "m;-lpthread"
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libopenjp2.so.2.3.1"
  IMPORTED_SONAME_RELEASE "libopenjp2.so.7"
  )

list(APPEND _IMPORT_CHECK_TARGETS openjp2 )
list(APPEND _IMPORT_CHECK_FILES_FOR_openjp2 "${_IMPORT_PREFIX}/lib/libopenjp2.so.2.3.1" )

# Import target "opj_decompress" for configuration "RELEASE"
set_property(TARGET opj_decompress APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(opj_decompress PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/bin/opj_decompress"
  )

list(APPEND _IMPORT_CHECK_TARGETS opj_decompress )
list(APPEND _IMPORT_CHECK_FILES_FOR_opj_decompress "${_IMPORT_PREFIX}/bin/opj_decompress" )

# Import target "opj_compress" for configuration "RELEASE"
set_property(TARGET opj_compress APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(opj_compress PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/bin/opj_compress"
  )

list(APPEND _IMPORT_CHECK_TARGETS opj_compress )
list(APPEND _IMPORT_CHECK_FILES_FOR_opj_compress "${_IMPORT_PREFIX}/bin/opj_compress" )

# Import target "opj_dump" for configuration "RELEASE"
set_property(TARGET opj_dump APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(opj_dump PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/bin/opj_dump"
  )

list(APPEND _IMPORT_CHECK_TARGETS opj_dump )
list(APPEND _IMPORT_CHECK_FILES_FOR_opj_dump "${_IMPORT_PREFIX}/bin/opj_dump" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
