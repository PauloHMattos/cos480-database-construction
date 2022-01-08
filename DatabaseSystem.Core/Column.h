#pragma once
#include "ColumnType.h"

struct Column
{
public:
	string Name;
	ColumnType Type;
	unsigned int ArraySize;

	Column(string name, ColumnType type, unsigned int arraySize);
	unsigned int GetLength() const;


	static int Compare(const Column& column, span<unsigned char> a, span<unsigned char> b);
	static bool Equals(const Column& column, span<unsigned char> a, span<unsigned char> b);
	static void Parse(const Column& column, span<unsigned char> destination, string str);
	static void WriteValue(ostream& out, const Column& column, span<unsigned char> destination);
};