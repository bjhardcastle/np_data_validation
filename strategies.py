"""Strategies for looking up DataValidationFiles in a database and performing some action depending on context."""


from typing import List

import data_validation as dv


def new_file_with_checksum(subject: dv.DataValidationFile) -> dv.DataValidationFile:
    """
    Get a new file to avoid modifying the original.
    """
    checksum = subject.generate_checksum(subject.path, subject.size)
    return dv.DataValidationFile(path=subject.path, size=subject.size, checksum=checksum)


def generate_checksum(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    Generate a checksum for a file and add to database.
    """
    new_file = new_file_with_checksum(subject)
    db.add_file(new_file)
    return new_file


def generate_checksum_if_not_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB):
    """
    If the database has no entry for the subject file, generate a checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    matches = db.get_matches(subject, match=accepted_matches)
    if not matches:
        generate_checksum(subject, db)

    
def find_invalid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for invalid copies of the subject file in database.
    """
    matches, match_type = db.get_matches(subject)
    return [
        m for i,m in enumerate(matches) 
        if match_type[i] >= subject.Match.CHECKSUM_COLLISION 
        and match_type[i] <= subject.Match.UNSYNCED_OR_CORRUPT_DATA
        ] or None


def find_valid_copies_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> List[dv.DataValidationFile]:
    """
    Check for valid copies of the subject file in database.
    """
    accepted_matches = [subject.Match.VALID_COPY_RENAMED, subject.Match.VALID_COPY_SAME_NAME]
    matches = db.get_matches(subject, match=accepted_matches)
    return matches or None
    

def exchange_if_checksum_in_db(subject: dv.DataValidationFile, db: dv.DataValidationDB) -> dv.DataValidationFile:
    """
    If the database has an entry for the subject file that already has a checksum, swap 
    the subject for the entry in the database. Saves us regenerating checksums for large files.
    If not, return the subject.
    """
    if subject.checksum:
        return subject
    
    accepted_matches = subject.Match.SELF_NO_CHECKSUM
    matches = db.get_matches(subject, match=accepted_matches)
    
    if not matches:
        return subject
    
    if len(matches) == 1:
        return matches[0]
    else:
        # multiple matches with different checksums: should regenerate checksum
        return subject


def delete_if_valid_backup_in_db(subject: dv.DataValidationFile, matches: List[dv.DataValidationFile]) -> None:
    """
    If the database has an entry for the subject file in known backup locations, or a new specified location, we can
    delete the subject.
    """
    accepted_matches = [subject.Match.VALID_COPY_RENAMED, subject.Match.VALID_COPY_SAME_NAME]
    #TODO check for accepted matches that contain backup locations in path
    #* this requires subject has checksum
    
    
def find_backup_if_not_in_db(subject: dv.DataValidationFile, matches: List[dv.DataValidationFile]) -> None:
    """
    If the database has no matches in backup locations we can go looking in lims/npexp/specified folder.
    """
    # accepted_matches >= [subject.Match.CHECKSUM_COLLISION]
    # a match could be anything here: but if it doesn't have a checksum we need to generate it 
      
def regenerate_checksums_on_mismatch(subject: dv.DataValidationFile, other: dv.DataValidationFile) -> None:
    """
    If the database has an entry for the subject file that has a different checksum, regenerate the checksum for it.
    """
    accepted_matches = [subject.Match.SELF, subject.Match.SELF_NO_CHECKSUM]
    #TODO regenerate and check again
    #* need a solution for replacing entries where the checksum is different

