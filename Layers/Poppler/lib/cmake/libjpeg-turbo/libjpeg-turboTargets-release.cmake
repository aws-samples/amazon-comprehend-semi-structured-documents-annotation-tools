#----------------------------------------------------------------
# Generated CMake target import file for configuration "RELEASE".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "libjpeg-turbo::jpeg" for configuration "RELEASE"
set_property(TARGET libjpeg-turbo::jpeg APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(libjpeg-turbo::jpeg PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libjpeg.so.62.3.0"
  IMPORTED_SONAME_RELEASE "libjpeg.so.62"
  )

list(APPEND _IMPORT_CHECK_TARGETS libjpeg-turbo::jpeg )
list(APPEND _IMPORT_CHECK_FILES_FOR_libjpeg-turbo::jpeg "${_IMPORT_PREFIX}/lib/libjpeg.so.62.3.0" )

# Import target "libjpeg-turbo::turbojpeg" for configuration "RELEASE"
set_property(TARGET libjpeg-turbo::turbojpeg APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(libjpeg-turbo::turbojpeg PROPERTIES
  IMPORTED_LOCATION_RELEASE "${_IMPORT_PREFIX}/lib/libturbojpeg.so.0.2.0"
  IMPORTED_SONAME_RELEASE "libturbojpeg.so.0"
  )

list(APPEND _IMPORT_CHECK_TARGETS libjpeg-turbo::turbojpeg )
list(APPEND _IMPORT_CHECK_FILES_FOR_libjpeg-turbo::turbojpeg "${_IMPORT_PREFIX}/lib/libturbojpeg.so.0.2.0" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
