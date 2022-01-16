#pragma once
#include "../DatabaseSystem.Core/FileHead.h"

class OrderedFileHead : public FileHead
{
public:
	OrderedFileHead(Schema *schema);
	~OrderedFileHead();

	// Inherited via FileHead
	virtual void Serialize(iostream &dst) override;
	virtual void Deserialize(iostream &src) override;
};