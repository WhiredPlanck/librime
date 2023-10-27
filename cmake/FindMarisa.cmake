set(_marisa_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES})

find_path(Marisa_INCLUDE_PATH marisa.h)

if (WITH_STATIC_DEPS)
  if (WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .lib ${CMAKE_FIND_LIBRARY_SUFFIXES})
  else (WIN32)
    set(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
  endif (WIN32)
endif ()
find_library(Marisa_LIBRARY NAMES marisa libmarisa)
if(Marisa_INCLUDE_PATH AND Marisa_LIBRARY)
  set(Marisa_FOUND TRUE)
endif(Marisa_INCLUDE_PATH AND Marisa_LIBRARY)
if(Marisa_FOUND)
  if(NOT Marisa_FIND_QUIETLY)
    message(STATUS "Found marisa: ${Marisa_LIBRARY}")
  endif(NOT Marisa_FIND_QUIETLY)
else(Marisa_FOUND)
  if(Marisa_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find marisa library.")
  endif(Marisa_FIND_REQUIRED)
endif(Marisa_FOUND)

set(CMAKE_FIND_LIBRARY_SUFFIXES ${_marisa_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES})
